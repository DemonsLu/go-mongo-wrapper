package mongoWrapper

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"strings"
)

/**
 * Created by Demons.
 * Contact: <lu.xu@zenjoy.net>
 * Date: 2021/1/14 14:30
 * Let's go~
 */

func NewClient(opts ...*options.ClientOptions) (client *Client, err error) {
	mongoClient, err := mongo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	err = mongoClient.Connect(context.Background())
	if err != nil {
		return nil, err
	}

	err = mongoClient.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return
	}
	client = &Client{mongoClient}
	return
}

type Client struct {
	*mongo.Client
}

type MongoExecutor struct {
	cli        *Client
	dbName     string
	collection string
	filter     bson.M
	updater    bson.M
}

func (c *Client) NewExecutor() (exec *MongoExecutor) {
	exec = &MongoExecutor{cli: c, filter: bson.M{}, updater: bson.M{}}
	return
}

func (e *MongoExecutor) SetDBName(dbName string) *MongoExecutor {
	e.dbName = dbName
	return e
}

func (e *MongoExecutor) SetCollection(coll string) *MongoExecutor {
	e.collection = coll
	return e
}

func (e *MongoExecutor) Where(where string, whereValues ...interface{}) *MongoExecutor {
	filter := parseQuery(where, whereValues)
	for k, v := range filter {
		e.filter[k] = v
	}
	return e
}

func (e *MongoExecutor) Or(where string, whereValues ...interface{}) *MongoExecutor {
	filter := parseQuery(where, whereValues)
	e.filter["$or"] = bson.A{filter}
	return e
}

func (e *MongoExecutor) UpdaterSet(updater map[string]interface{}) *MongoExecutor {
	e.updater["$set"] = updater
	return e
}

func (e *MongoExecutor) UpdaterInc(inc map[string]interface{}) *MongoExecutor {
	e.updater["$inc"] = inc
	return e
}

func (e *MongoExecutor) InsertOne(document interface{}, opts ...*options.InsertOneOptions) (result *mongo.InsertOneResult, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).InsertOne(context.Background(), document, opts...)
}

func (e *MongoExecutor) Find(findOptions ...*options.FindOptions) (cursor *mongo.Cursor, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).Find(context.Background(), e.filter, findOptions...)
}

func (e *MongoExecutor) FindOne(findOptions ...*options.FindOneOptions) (result *mongo.SingleResult) {
	return e.cli.Database(e.dbName).Collection(e.collection).FindOne(context.Background(), e.filter, findOptions...)
}

func (e *MongoExecutor) FindOneAndDelete(opts ...*options.FindOneAndDeleteOptions) (result *mongo.SingleResult) {
	return e.cli.Database(e.dbName).Collection(e.collection).FindOneAndDelete(context.Background(), e.filter, opts...)
}

func (e *MongoExecutor) FindOneAndUpdate(opts ...*options.FindOneAndUpdateOptions) (result *mongo.SingleResult) {
	return e.cli.Database(e.dbName).Collection(e.collection).FindOneAndUpdate(context.Background(), e.filter, e.updater, opts...)
}

func (e *MongoExecutor) FindOneAndReplace(replacement interface{}, opts ...*options.FindOneAndReplaceOptions) (result *mongo.SingleResult) {
	return e.cli.Database(e.dbName).Collection(e.collection).FindOneAndReplace(context.Background(), e.filter, replacement, opts...)
}

func (e *MongoExecutor) ReplaceOne(replacement interface{}, replaceOptions ...*options.ReplaceOptions) (result *mongo.UpdateResult, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).ReplaceOne(context.Background(), e.filter, replacement, replaceOptions...)
}

func (e *MongoExecutor) UpdateOne(updateOptions ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).UpdateOne(context.Background(), e.filter, e.updater, updateOptions...)
}

func (e *MongoExecutor) UpdateMany(updateOptions ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).UpdateMany(context.Background(), e.filter, e.updater, updateOptions...)
}

func (e *MongoExecutor) DeleteOne(deleteOptions ...*options.DeleteOptions) (result *mongo.DeleteResult, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).DeleteOne(context.Background(), e.filter, deleteOptions...)
}

func (e *MongoExecutor) DeleteMany(deleteOptions ...*options.DeleteOptions) (result *mongo.DeleteResult, err error) {
	return e.cli.Database(e.dbName).Collection(e.collection).DeleteMany(context.Background(), e.filter, deleteOptions...)
}

func parseQuery(query string, values []interface{}) (result bson.M) {

	result = bson.M{}
	singleQuerys := strings.Split(query, " and ")
	valueIndex := 0

	doParse := func(querySplit []string) (param string, value interface{}) {
		param, v := querySplit[0], querySplit[1]
		if v == "?" {
			value = values[valueIndex]
			valueIndex++
		} else {
			value = v
		}
		return
	}

	for _, singleQuery := range singleQuerys {
		var querySplit []string
		if querySplit = strings.Split(singleQuery, " >= "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = bson.M{"$gte": value}
		} else if querySplit = strings.Split(singleQuery, " <= "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = bson.M{"$lte": value}
		} else if querySplit = strings.Split(singleQuery, " > "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = bson.M{"$gt": value}
		} else if querySplit = strings.Split(singleQuery, " < "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = bson.M{"$lt": value}
		} else if querySplit = strings.Split(singleQuery, " nin "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = bson.M{"$nin": value}
		} else if querySplit = strings.Split(singleQuery, " in "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = bson.M{"$in": value}
		} else if querySplit = strings.Split(singleQuery, " = "); len(querySplit) == 2 {
			param, value := doParse(querySplit)
			result[param] = value
		}
	}
	return
}
