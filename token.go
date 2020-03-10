package ddbstore

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/oauth2.v3"
	"gopkg.in/oauth2.v3/models"
)

// TokenConfig token configuration parameters
type TokenConfig struct {
	// store token based data collection name(The default is oauth2_basic)
	BasicCName string
	// store access token data collection name(The default is oauth2_access)
	AccessCName string
	// store refresh token data collection name(The default is oauth2_refresh)
	RefreshCName string
}

// TokenStore stores the dynamodb client and token config
type TokenStore struct {
	tcfg   *TokenConfig
	client *dynamodb.DynamoDB
}

type basicData struct {
	ID        string    `bson:"_id"`
	Data      []byte    `bson:"Data"`
	ExpiredAt time.Time `bson:"ExpiredAt"`
}

type tokenData struct {
	ID        string    `bson:"_id"`
	BasicID   string    `bson:"BasicID"`
	ExpiredAt time.Time `bson:"ExpiredAt"`
}

func initTable(client *dynamodb.DynamoDB, tokenConfig *TokenConfig) (err error) {
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
		TableName: &tokenConfig.BasicCName,
	}
	_, err = client.CreateTable(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				break
			default:
				fmt.Println("Got error calling CreateTable for authorization code:")
				fmt.Println(awsErr.Error())
				os.Exit(1)
			}
		}
	}

	// Create access token table
	input = &dynamodb.CreateTableInput{
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
		TableName: &tokenConfig.AccessCName,
	}
	_, err = client.CreateTable(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				break
			default:
				fmt.Println("Got error calling CreateTable for access token:")
				fmt.Println(awsErr.Error())
				os.Exit(1)
			}
		}
	}

	// Create refresh token table
	input = &dynamodb.CreateTableInput{
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
		TableName: &tokenConfig.RefreshCName,
	}

	_, err = client.CreateTable(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				break
			default:
				fmt.Println("Got error calling CreateTable for refresh token:")
				fmt.Println(awsErr.Error())
				os.Exit(1)
			}
		}
	}
	return
}

// NewTokenStore returns a new token store
func NewTokenStore(client *dynamodb.DynamoDB, tokenConfig *TokenConfig) (store *TokenStore) {
	initTable(client, tokenConfig)
	store = &TokenStore{
		tcfg:   tokenConfig,
		client: client,
	}
	return
}

// NewDefaultTokenConfig returns a default token config
func NewDefaultTokenConfig() (config *TokenConfig) {
	config = &TokenConfig{
		BasicCName:   "oauth2_basic",
		AccessCName:  "oauth2_access",
		RefreshCName: "oauth2_refresh",
	}
	return
}

// InsertAuthorizationCode inserts an authorization code into the basic table
func InsertAuthorizationCode(ts *TokenStore, code string, data []byte, expiresAt string) (err error) {
	params := &dynamodb.PutItemInput{
		TableName: aws.String(ts.tcfg.BasicCName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID": &dynamodb.AttributeValue{
				S: aws.String(code),
			},
			"Data": &dynamodb.AttributeValue{
				B: data,
			},
			"ExpiredAt": &dynamodb.AttributeValue{
				S: aws.String(expiresAt),
			},
		},
	}
	_, err = ts.client.PutItem(params)
	return
}

// InsertAccessToken inserts an access code into the basic table
func InsertAccessToken(ts *TokenStore, token string, basicID string, expiresAt string) (err error) {
	params := &dynamodb.PutItemInput{
		TableName: aws.String(ts.tcfg.AccessCName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID": &dynamodb.AttributeValue{
				S: aws.String(token),
			},
			"BasicID": &dynamodb.AttributeValue{
				S: aws.String(basicID),
			},
			"ExpiredAt": &dynamodb.AttributeValue{
				S: aws.String(expiresAt),
			},
		},
	}
	_, err = ts.client.PutItem(params)
	return
}

// InsertRefreshToken inserts an access code into the basic table
func InsertRefreshToken(ts *TokenStore, token string, basicID string, expiresAt string) (err error) {
	refreshParams := &dynamodb.PutItemInput{
		TableName: aws.String(ts.tcfg.RefreshCName),
		Item: map[string]*dynamodb.AttributeValue{
			"ID": &dynamodb.AttributeValue{
				S: aws.String(token),
			},
			"BasicID": &dynamodb.AttributeValue{
				S: aws.String(basicID),
			},
			"ExpiredAt": &dynamodb.AttributeValue{
				S: aws.String(expiresAt),
			},
		},
	}
	_, err = ts.client.PutItem(refreshParams)
	return
}

// Create creates
func (ts *TokenStore) Create(info oauth2.TokenInfo) (err error) {
	data, err := json.Marshal(info)
	if err != nil {
		return
	}

	if code := info.GetCode(); code != "" {
		// Code already exists, update basic table
		exp := info.GetCodeCreateAt().Add(info.GetCodeExpiresIn()).Format(time.RFC3339)
		err = InsertAuthorizationCode(ts, code, data, exp)
		return
	}

	aexp := info.GetAccessCreateAt().Add(info.GetAccessExpiresIn())
	rexp := aexp
	if refresh := info.GetRefresh(); refresh != "" {
		rexp = info.GetRefreshCreateAt().Add(info.GetRefreshExpiresIn())
		if aexp.Second() > rexp.Second() {
			aexp = rexp
		}
	}
	id := bson.NewObjectId().Hex()
	// Update basic table with new token
	err = InsertAuthorizationCode(ts, id, data, rexp.Format(time.RFC3339))
	if err != nil {
		return
	}
	err = InsertAccessToken(ts, info.GetAccess(), id, aexp.Format(time.RFC3339))
	if err != nil {
		return
	}
	if refresh := info.GetRefresh(); refresh != "" {
		err = InsertRefreshToken(ts, refresh, id, rexp.Format(time.RFC3339))
	}
	return
}

// RemoveByCode use the authorization code to delete the token information
func (ts *TokenStore) RemoveByCode(code string) (err error) {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(code),
			},
		},
		TableName: aws.String(ts.tcfg.BasicCName),
	}
	_, err = ts.client.DeleteItem(input)
	if err != nil {
		fmt.Printf("RemoveByCode error: %s\n", err.Error())
	}
	return
}

// RemoveByAccess use the access token to delete the token information
func (ts *TokenStore) RemoveByAccess(access string) (err error) {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(access),
			},
		},
		TableName: aws.String(ts.tcfg.AccessCName),
	}
	_, err = ts.client.DeleteItem(input)
	if err != nil {
		fmt.Printf("RemoveByAccess error: %s\n", err.Error())
	}
	return
}

// RemoveByRefresh use the refresh token to delete the token information
func (ts *TokenStore) RemoveByRefresh(refresh string) (err error) {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(refresh),
			},
		},
		TableName: aws.String(ts.tcfg.RefreshCName),
	}
	_, err = ts.client.DeleteItem(input)
	if err != nil {
		fmt.Printf("RemoveByRefresh error: %s\n", err.Error())
	}
	return
}

func (ts *TokenStore) getData(basicID string) (ti oauth2.TokenInfo, err error) {
	if len(basicID) == 0 {
		return
	}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(ts.tcfg.BasicCName),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(basicID),
			},
		},
	}
	result, err := ts.client.GetItem(input)
	if err != nil {
		return
	}
	if len(result.Item) == 0 {
		return
	}
	var b basicData
	err = dynamodbattribute.UnmarshalMap(result.Item, &b)
	if err != nil {
		return
	}
	var tm models.Token
	err = json.Unmarshal(b.Data, &tm)
	if err != nil {
		return
	}
	ti = &tm
	return
}

func (ts *TokenStore) getBasicID(cname, token string) (basicID string, err error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(token),
			},
		},
		TableName: aws.String(cname),
	}
	result, err := ts.client.GetItem(input)
	if err != nil {
		return
	}
	var td tokenData
	err = dynamodbattribute.UnmarshalMap(result.Item, &td)
	if err != nil {
		return
	}
	basicID = td.BasicID
	return
}

// GetByCode use the authorization code for token information data
func (ts *TokenStore) GetByCode(code string) (ti oauth2.TokenInfo, err error) {
	ti, err = ts.getData(code)
	return
}

// GetByAccess use the access token for token information data
func (ts *TokenStore) GetByAccess(access string) (ti oauth2.TokenInfo, err error) {
	basicID, err := ts.getBasicID(ts.tcfg.AccessCName, access)
	if err != nil && basicID == "" {
		return
	}
	ti, err = ts.getData(basicID)
	return
}

// GetByRefresh use the refresh token for token information data
func (ts *TokenStore) GetByRefresh(refresh string) (ti oauth2.TokenInfo, err error) {
	basicID, err := ts.getBasicID(ts.tcfg.RefreshCName, refresh)
	if err != nil && basicID == "" {
		return
	}
	ti, err = ts.getData(basicID)
	return
}
