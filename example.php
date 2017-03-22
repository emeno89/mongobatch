<?php

//timezone
date_default_timezone_set("Europe/Moscow");

require_once 'vendor/autoload.php';

//create and setup mongo client
$mongoClient = new \MongoClient();

/**
 * create and setup provider client (i will use inbox redis and his CacheInterface provider),
 * but you can create your own provider, which implements CacheInterface (@see http://www.php-fig.org/psr/psr-16/)
 * and pass them to MongoBatch constructor for enable caching
 */
//$cacheClient = new \MongoBatch\CacheProvider\RedisCache(new \Predis\Client());
$cacheClient = null;

//use logger for log this example
$logger = new \Monolog\Logger('batch.logger');

//create MongoBatch instance
$mongoBatch = new \MongoBatch\MongoBatch($mongoClient, $cacheClient);

/*
 * for example i will use collection proj.users, structure of document
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

    $mongoBatch
        ->setIterationField('_id', 1)           //batch by _id ascending
        ->setDbName('proj')                     //for database proj
        ->setCollectionName('users')            //for collection users
        ->setFilter($filter)                    //set filter there
        ->setSaveState(true)                    //enable save state
        ->setSaveStateSeconds(60 * 60 * 3)      //set save state to cache seconds = 3 hours
        ->setBatchSize(200)                     //set batch size = 200
        ->setPause(0.05)                        //and set pause 50ms
        ->setCalcCount(true);                   //enable calculation of all query count

    /**
     * @WARNING: avoid calculation (setCalcCount(true)) for not indexed queries on large collection,
     * because your query will be slow and can dramatically decrease performance of mongo instance
     */

    //and in final case we execute our batch with anonymous function with $data array argument for every document
    $mongoBatch->execute(function($data, $currentCounter, $queryCount) use ($logger){

        $logger->info('data: ', $data);
        $logger->info('current: '. $currentCounter);
        $logger->info('total: '.$queryCount);

    });

}
catch(\Exception $ex){
    $logger->error('mongo batch error: '.get_class($ex).': '.$ex->getMessage());
}

/**
 * If your script will be crashed or something will be wrong, you can restart them and MongoBatch will start from
 * last document, which contains last iteration field value
 */
