<?php

/**
 * Example script
 * @author Dmitriy Dryutskiy <emeno@yandex.ru>
 */

//timezone
date_default_timezone_set("Europe/Moscow");

require_once 'vendor/autoload.php';

//create and setup mongo client
$mongoClient = new MongoClient();

//get collection object and set db name and collection name
$mongoCollection = $mongoClient->selectCollection('proj', 'users');

/**
 * You can use redis (\MongoBatch\Provider\RedisCache) as cache provider,
 * but you can create your own provider, which implements \Psr\SimpleCache\CacheInterface
 * (@see http://www.php-fig.org/psr/psr-16/) and pass it to MongoBatch constructor for enable caching
 */

$redisConfig = array(
    'redis' => array("tcp://localhost:6379")
);

$redisClient = new \Predis\Client($redisConfig);

$cacheProvider = new \MongoBatch\CacheProvider\RedisCache($redisClient);

//use logger for log this example
$logger = new \Monolog\Logger('mongo.batch.logger');

//you can prepare mongo collection options before passing it to MongoBatch
$mongoCollection->setReadPreference(MongoClient::RP_SECONDARY_PREFERRED);

//create MongoBatch instance
$mongoBatch = new \MongoBatch\MongoBatch($mongoCollection, $cacheProvider);

/*
 * for example i will use collection proj.users with structure of document:
 * {
 *     "_id": ObjectId(...)
 *     "name": "UserName",
 *     "last_name": "UserLastName"
 *     "email": "nick@domain.org"
 *     "is_active": true
 * }
 *
 * and i want to get only active users
 *
 */
$filter = array('is_active' => true);

try {

    /**
     * @WARNING: you must avoid count results (setCalcCount(true)) for not indexed queries on large collection,
     * because your query will be slow and can dramatically decrease performance of mongo instance
     */

    $mongoBatch
        ->setIterationField('_id', -1)          //batch by _id ascending
        ->setFilter($filter)                    //set filter there
        ->setSaveState(true)                    //enable save state
        ->setSaveStateSeconds(60 * 60 * 3)      //set save state to cache seconds = 3 hours
        ->setBatchSize(200)                     //set batch size = 200
        ->setPause(0.05)                        //and set pause 50ms
        ->setCalcCount(true);                   //enable calculation of all query count

    //and in final case we execute our batch with anonymous function with $data array argument for every document
    $mongoBatch->execute(function($data, $currentCounter, $queryCount) use ($logger){
        $logger->info('data: '.json_encode($data).', current: '.$currentCounter.', total: '.$queryCount);
    });
}
catch(\Exception $ex){
    $logger->error('mongo batch error: '.get_class($ex).': '.$ex->getMessage());
}

/**
 * If your script will crashed or something will be wrong, you can restart it and MongoBatch will get data since
 * last document, which contains last iteration field value
 */
