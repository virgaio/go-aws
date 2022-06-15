package dynamodb

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

var client *dynamodb.DynamoDB
var mtx sync.Mutex

type Item map[string]*dynamodb.AttributeValue
type Items []map[string]*dynamodb.AttributeValue

type Attributes map[string]interface{}

type ItemBase struct {
	TableName    string
	HashKeyName  string
	HashKeyValue interface{}
	SortKeyName  string
	SortKeyValue interface{}
	ItemTypeName string
	Attributes   Attributes
}

type ItemWriterInput struct {
	ItemBase
}

type ItemCreateInput struct {
	ItemBase
}

type ItemReaderInput struct {
	IndexName string
	ItemBase
}

func newItemBase(itemTypeName, tableName, hashKeyName, sortKeyName string, hashKeyValue, sortKeyValue interface{}) ItemBase {
	i := ItemBase{
		ItemTypeName: itemTypeName,
		TableName:    tableName,
		HashKeyName:  hashKeyName,
		HashKeyValue: hashKeyValue,
		Attributes:   make(map[string]interface{}),
	}
	if sortKeyName != "" {
		i.SortKeyName = sortKeyName
		i.SortKeyValue = sortKeyValue
	}
	return i
}

func NewItemWriterInput(itemTypeName, tableName, hashKeyName, sortKeyName string, hashKeyValue, sortKeyValue interface{}) *ItemWriterInput {
	return &ItemWriterInput{
		ItemBase: newItemBase(itemTypeName, tableName, hashKeyName, sortKeyName, hashKeyValue, sortKeyValue),
	}
}

func NewItemReaderInput(itemTypeName, tableName, hashKeyName, sortKeyName string, hashKeyValue, sortKeyValue interface{}) *ItemReaderInput {
	return &ItemReaderInput{
		ItemBase: newItemBase(itemTypeName, tableName, hashKeyName, sortKeyName, hashKeyValue, sortKeyValue),
	}
}

func getAttributeValue(input interface{}) *dynamodb.AttributeValue {
	if input == nil {
		return nil
	}
	switch input.(type) {
	case string:
		return &dynamodb.AttributeValue{
			S: aws.String(input.(string)),
		}
	case int:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(int64(input.(int)), 10)),
		}
	case int64:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(input.(int64), 10)),
		}
	case float32:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatFloat(float64(input.(float32)), 'f', 4, 32)),
		}
	case float64:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatFloat(input.(float64), 'f', 4, 64)),
		}
	}
	return nil
}

func (a Attributes) ToItem() Item {
	i := make(Item)
	for k, v := range a {
		av := getAttributeValue(v)
		if av != nil {
			i[k] = av
		}
	}
	return i
}

func (i Item) Merge(i2 Item) {
	for k, v := range i2 {
		i[k] = v
	}
}

func (i *ItemBase) GetKey() Item {
	key := make(Item)
	if av := getAttributeValue(i.HashKeyValue); av != nil {
		key[i.HashKeyName] = av
	}
	if av := getAttributeValue(i.SortKeyValue); av != nil {
		key[i.SortKeyName] = av
	}
	return key
}

func (i *ItemBase) GetPuttableItem() Item {
	var item Item
	if i.Attributes != nil {
		item = i.Attributes.ToItem()
	}
	if item == nil {
		item = i.GetKey()
	} else {
		item.Merge(i.GetKey())
	}
	return item
}

func (i *ItemWriterInput) Update(sess *session.Session) (*dynamodb.UpdateItemOutput, error) {
	connect(sess)
	builder := expression.UpdateBuilder{}
	for k, v := range i.Attributes {
		builder = builder.Set(expression.Name(k),
			expression.Value(v))
	}
	return i.UpdateWithBuilder(sess, &builder)
}

func (i *ItemWriterInput) UpdateWithBuilder(sess *session.Session, builder *expression.UpdateBuilder) (*dynamodb.UpdateItemOutput, error) {
	connect(sess)

	expr, err := expression.NewBuilder().WithUpdate(*builder).Build()
	if err != nil {
		return nil, err
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(i.TableName),
		Key:                       i.GetKey(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	}

	output, err := client.UpdateItem(input)
	if err != nil {
		return output, err
	}
	return output, nil
}

func (i *ItemWriterInput) CreateWithItem(sess *session.Session,
	item Item) (*dynamodb.PutItemOutput, error) {
	connect(sess)
	item.Merge(i.GetKey())
	condition := "attribute_not_exists(id)"
	putInput := &dynamodb.PutItemInput{
		Item:                item,
		TableName:           aws.String(i.TableName),
		ConditionExpression: &condition,
	}
	out, err := client.PutItem(putInput)
	if err != nil {
		return out, err
	}
	return out, nil
}

func (i *ItemWriterInput) Create(sess *session.Session) (*dynamodb.PutItemOutput, error) {
	connect(sess)
	return i.CreateWithItem(sess, i.GetPuttableItem())
}

func (i *ItemWriterInput) Upsert(sess *session.Session) (*dynamodb.PutItemOutput, error) {
	connect(sess)
	putInput := &dynamodb.PutItemInput{
		Item:      i.GetPuttableItem(),
		TableName: aws.String(i.TableName),
	}
	out, err := client.PutItem(putInput)
	if err != nil {
		return out, err
	}
	return out, nil

}

func (i *ItemWriterInput) Delete(sess *session.Session) (*dynamodb.DeleteItemOutput, error) {
	connect(sess)
	input := &dynamodb.DeleteItemInput{
		Key:       i.GetKey(),
		TableName: aws.String(i.TableName),
	}
	out, err := client.DeleteItem(input)
	if err != nil {
		return out, err
	}
	return out, nil

}

func (i *ItemReaderInput) Get(sess *session.Session) (*dynamodb.GetItemOutput, error) {
	connect(sess)
	input := &dynamodb.GetItemInput{
		TableName: aws.String(i.TableName),
		Key:       i.GetKey(),
	}
	result, err := client.GetItem(input)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Item == nil {
		return result, fmt.Errorf("%s does not exist: [%v:%v]", i.ItemTypeName, i.HashKeyValue, i.SortKeyValue)
	}
	return result, nil
}

func (i *ItemReaderInput) List(sess *session.Session, limit int64, reverse bool) (*dynamodb.QueryOutput, error) {
	connect(sess)
	kc := make(map[string]*dynamodb.Condition)
	key := i.GetKey()
	for k, v := range key {
		if k == "" {
			continue
		}
		kc[k] = &dynamodb.Condition{
			ComparisonOperator: aws.String("EQ"),
			AttributeValueList: []*dynamodb.AttributeValue{
				v,
			},
		}
	}
	var queryInput = &dynamodb.QueryInput{
		TableName:        aws.String(i.TableName),
		KeyConditions:    kc,
		ScanIndexForward: aws.Bool(!reverse),
	}
	if i.IndexName != "" {
		queryInput.IndexName = aws.String(i.IndexName)
	}
	if limit > 0 {
		queryInput.Limit = aws.Int64(limit)
	}
	return client.Query(queryInput)
}

func (i *ItemReaderInput) Scan(sess *session.Session, limit int64) (*dynamodb.ScanOutput, error) {
	connect(sess)
	params := &dynamodb.ScanInput{
		TableName: aws.String(i.TableName),
	}
	if i.IndexName != "" {
		params.IndexName = aws.String(i.IndexName)
	}
	if limit > 0 {
		params.Limit = aws.Int64(limit)
	}
	return client.Scan(params)
}

func connect(sess *session.Session) {
	if client == nil {
		mtx.Lock()
		// once mutex is acquired, re-check for nil in case another
		// invocation was also called and got the lock first
		if client == nil {
			client = dynamodb.New(sess)
		}
		mtx.Unlock()
	}
}
