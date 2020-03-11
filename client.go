package dynamodb

import (
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"gopkg.in/oauth2.v3"
	"gopkg.in/oauth2.v3/models"
)

// ClientConfig client configuration parameters
type ClientConfig struct {
	// store clients data collection name(The default is oauth2_clients)
	ClientsCName string
}

// ClientStore DynamoDB storage for OAuth 2.0
type ClientStore struct {
	ccfg   *ClientConfig
	dbName string
	client *dynamodb.DynamoDB
}

// NewDefaultClientConfig create a default client configuration
func NewDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ClientsCName: "oauth2_clients",
	}
}

func initClientTable(client *dynamodb.DynamoDB, clientConfig *ClientConfig) (err error) {
	// Create authorization code table
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: &clientConfig.ClientsCName,
	}
	_, err = client.CreateTable(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				break
			default:
				fmt.Println("Got error calling CreateTable for clients:")
				fmt.Println(awsErr.Error())
				os.Exit(1)
			}
		}
	}
	return
}

// NewClientStore create a client store instance based on dynamodb
func NewClientStore(client *dynamodb.DynamoDB, ccfg *ClientConfig) (store *ClientStore) {
	initClientTable(client, ccfg)
	store = &ClientStore{
		ccfg:   ccfg,
		client: client,
	}
	return
}

// Set set client information
func (cs *ClientStore) Set(info oauth2.ClientInfo) (err error) {
	params := &dynamodb.PutItemInput{
		TableName: aws.String(cs.ccfg.ClientsCName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID": &dynamodb.AttributeValue{
				S: aws.String(info.GetID()),
			},
			"Secret": &dynamodb.AttributeValue{
				S: aws.String(info.GetSecret()),
			},
			"Domain": &dynamodb.AttributeValue{
				S: aws.String(info.GetDomain()),
			},
			"UserID": &dynamodb.AttributeValue{
				S: aws.String(info.GetUserID()),
			},
		},
		ConditionExpression: aws.String("attribute_not_exists(ID)"),
	}
	_, err = cs.client.PutItem(params)

	return
}

// GetByID according to the ID for the client information
func (cs *ClientStore) GetByID(id string) (info oauth2.ClientInfo, err error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(id),
			},
		},
		TableName: aws.String(cs.ccfg.ClientsCName),
	}
	result, err := cs.client.GetItem(input)
	if len(result.Item) == 0 {
		err = errors.New("no such client id")
		return
	}

	var infoC models.Client
	err = dynamodbattribute.UnmarshalMap(result.Item, &infoC)
	info = &infoC
	return
}

// RemoveByID use the client id to delete the client information
func (cs *ClientStore) RemoveByID(id string) (err error) {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(id),
			},
		},
		TableName:           aws.String(cs.ccfg.ClientsCName),
		ConditionExpression: aws.String("attribute_exists(ID)"),
	}
	_, err = cs.client.DeleteItem(input)
	return
}
