# go-oauth2-dynamodb
> Based on https://github.com/go-oauth2/mongo
> Based on https://github.com/contamobi/go-oauth2-dynamodb

## Install
```
$ go get -u github.com/rjewing/go-oauth2-dynamodb
```

## Usage
```
package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	
	ddbstore "github.com/rjewing/go-oauth2-dynamodb"
	"gopkg.in/oauth2.v3/manage"
)

func main() {
	manager := manage.NewDefaultManager()

	config := &aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	}
	sess := session.Must(session.NewSession(config))
	client := dynamodb.New(sess)

	// use dynamodb token store
	manager.MapTokenStorage(
	ddbstore.NewTokenStore(client, ddbstore.NewDefaultTokenConfig()),
	)
	// ...
}
```

## Testing
```
$ docker pull amazon/dynamodb-local
$ docker run -p 8000:8000 amazon/dynamodb-local
```

This sets up a local dynamodb server running on `http://localhost:8000`.

## MIT License
```
Copyright (c) 2020 Ryan Ewing
```
