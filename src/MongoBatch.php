<?php

namespace MongoBatch;

use MongoBatch\Exception\InvalidArgumentException;
use MongoBatch\Exception\RuntimeException;
use MongoBatch\Exception\UnexpectedValueException;
use Psr\SimpleCache\CacheInterface;

/**
 *
 * Class for batching large data sets in MongoDB (@see https://www.mongodb.com/what-is-mongodb)
 * and get data from collection for next
 *
 * Class MongoBatch
 * @package EmenoTools
 * @author Dmitriy Dryutskiy emeno@yandex.ru
 */
class MongoBatch
{

    /** @var \MongoClient $mongoClient */
    protected $mongoClient;

    /** @var CacheInterface $cacheClient */
    protected $cacheClient;

    /** @var int $iterationField */
    protected $iterationField;

    /** @var string|int $iterationSort */
    protected $iterationSort;

    /** @var string $dbName */
    protected $dbName;

    /** @var  string $collection */
    protected $collectionName;

    /** @var int $batchSize */
    protected $batchSize = 100;

    /** @var array $filter */
    protected $filter = array();

    /** @var array $fields */
    protected $fields = array();

    /** @var bool $saveState */
    protected $saveState = false;

    /** @var int $saveStateSeconds */
    protected $saveStateSeconds = 0;

    /** @var float $pause */
    protected $pause = 0.00;

    /** @var string */
    protected $cacheKeyPrefix = 'mongo:batch';

    /** @var bool $isClearIterationCache */
    protected $clearIterationCache = false;

    /** @var int $limit */
    protected $limit = -1;

    /** @var bool $calcCount */
    protected $calcCount = true;

    /**
     * MongoBatch constructor.
     * @param \MongoClient $_mongoClient
     * @param CacheInterface $_cacheClient
     */
    public function __construct(\MongoClient $_mongoClient, CacheInterface $_cacheClient)
    {
        $this->mongoClient = $_mongoClient;
        $this->cacheClient = $_cacheClient;
    }

    /**
     * @param string $iterationField
     * @param int|string $iterationSort
     * @return $this
     * @throws \MongoBatch\Exception\InvalidArgumentException
     */
    public function setIterationField($iterationField, $iterationSort = 1)
    {
        $this->iterationField = $iterationField;

        if (is_int($iterationSort)) {
            $this->iterationSort = $iterationSort == 1 ? 1 : -1;
        } else if (is_string($iterationSort)) {
            $this->iterationSort = $iterationSort == 'asc' ? 1 : -1;
        } else {
            throw new InvalidArgumentException('iterationSort', $iterationSort);
        }

        return $this;
    }

    /**
     * @param string $dbName
     * @return MongoBatch
     */
    public function setDbName($dbName)
    {
        $this->dbName = trim($dbName);
        if(empty($this->dbName)){
            throw new UnexpectedValueException('dbName', $this->dbName);
        }
        return $this;
    }

    /**
     * @return string
     */
    public function getDbName()
    {
        return $this->dbName;
    }

    /**
     * @param string $collectionName
     * @return MongoBatch
     */
    public function setCollectionName($collectionName)
    {
        $this->collectionName = trim($collectionName);
        if(empty($this->collectionName)){
            throw new UnexpectedValueException('collectionName', $this->collectionName);
        }
        return $this;
    }

    /**
     * @return string
     */
    public function getCollectionName()
    {
        return $this->collectionName;
    }

    /**
     * @param array $filter
     * @return MongoBatch
     */
    public function setFilter(array $filter = array())
    {
        $this->filter = $filter;
        return $this;
    }

    /**
     * @return array
     */
    public function getFilter()
    {
        return $this->filter;
    }

    /**
     * @param array $fields
     * @return static
     */
    public function setFields(array $fields = array())
    {
        $this->fields = $fields;
        return $this;
    }

    /**
     * @return array
     */
    public function getFields()
    {
        return $this->fields;
    }

    /**
     * @param int $batchSize
     * @return MongoBatch
     */
    public function setBatchSize($batchSize = 100)
    {
        $this->batchSize = (int)$batchSize;

        if($this->batchSize <= 1){
            throw new UnexpectedValueException('batchSize', $this->batchSize);
        }

        return $this;
    }

    /**
     * @return int
     */
    public function getBatchSize()
    {
        return (int)$this->batchSize;
    }

    /**
     * @param $saveState
     * @return MongoBatch
     */
    public function setSaveState($saveState)
    {
        $this->saveState = (bool)$saveState;
        return $this;
    }

    /**
     * @return bool
     */
    public function getSaveState()
    {
        return $this->saveState;
    }

    /**
     * @param float $pause
     * @return MongoBatch
     */
    public function setPause($pause)
    {
        $this->pause = (float)$pause;
        return $this;
    }

    /**
     * @return float
     */
    public function getPause()
    {
        return $this->pause;
    }

    /**
     * @param $seconds
     * @return MongoBatch
     */
    public function setSaveStateSeconds($seconds)
    {
        $this->saveStateSeconds = (int)$seconds;
        return $this;
    }

    /**
     * @return int
     */
    public function getLimit()
    {
        return $this->limit;
    }

    /**
     * @param int $limit
     * @return self
     */
    public function setLimit($limit)
    {
        $this->limit = (int)$limit;

        if($this->limit <= 0){
            throw new UnexpectedValueException('limit', $this->limit);
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function getClearIterationCache()
    {
        return $this->clearIterationCache;
    }

    /**
     * @param bool $clearIterationCache
     * @return $this
     */
    public function setClearIterationCache($clearIterationCache)
    {
        $this->clearIterationCache = (bool)$clearIterationCache;
        return $this;
    }

    /**
     * @return bool
     */
    public function getCalcCount()
    {
        return $this->calcCount;
    }

    /**
     * @param bool $calcCount
     */
    public function setCalcCount($calcCount)
    {
        $this->calcCount = (bool)$calcCount;
    }

    /**
     * @param $callbackFunction
     * @return int
     */
    public function execute($callbackFunction)
    {

        $this->ensureExecuteEnvironment($callbackFunction);

        $mongoCollection = $this->mongoClient->selectCollection($this->dbName, $this->collectionName);

        if(!$mongoCollection){
            throw new RuntimeException(__FUNCTION__, 'bad MongoCollection object');
        }

        $cacheKey = $this->prepareCacheKey();
        $iterationDirection = $this->iterationSort == 1 ? '$gt' : '$lt';

        if ($this->clearIterationCache && !empty($cacheKey)) {
            $this->cacheClient->delete($cacheKey);
        }

        $lastIterationFieldValue = null;
        if($this->saveState){
            $lastIterationFieldValue = (string)$this->cacheClient->get($cacheKey);
        }

        if($lastIterationFieldValue){
            $this->setSaveStateIterationField($lastIterationFieldValue, $iterationDirection);
        }

        $cursor = $mongoCollection->find($this->filter, $this->fields);

        $cursor->immortal(true);

        $cursor->sort([$this->iterationField => $this->iterationSort]);

        if ($this->limit) {
            $cursor->limit($this->getLimit());
        }

        $queryResultsCount = -1;

        if($this->getCalcCount()){
            $queryResultsCount = $cursor->count();
        }

        $documentCounter = 0;

        while($data = $cursor->getNext()){

            if(!isset($data[$this->iterationField]) || empty($data[$this->iterationField])){
                throw new RuntimeException(__FUNCTION__, "data[{$this->iterationField}] cannot be empty");
            }

            if(
                $documentCounter &&
                ($documentCounter % $this->batchSize) == 0 &&
                $this->pause > 0.00
            ){
                usleep($this->pause * 1000000);
            }

            if($queryResultsCount > 0 && $documentCounter >= $queryResultsCount){
                break;
            }

            $callbackData = array(
                $data,
                $documentCounter,
                $queryResultsCount
            );

            call_user_func_array($callbackFunction, $callbackData);

            if($this->saveState){
                $this->cacheClient->set($cacheKey, $data[$this->iterationField], $this->saveStateSeconds);
            }

            $documentCounter++;
        }

        return $documentCounter;
    }

    /**
     * @return string
     */
    protected function prepareCacheKey()
    {
        return $this->cacheKeyPrefix.":{$this->iterationField}:{$this->iterationSort}";
    }

    /**
     * @param mixed $lastIterationFieldValue
     * @param int $iterationDirection
     */
    protected function setSaveStateIterationField($lastIterationFieldValue, $iterationDirection)
    {
        if (isset($this->filter[$this->iterationField])) {

            $issetValuesArr = $this->filter[$this->iterationField];

            unset($this->filter[$this->iterationField]);

            if (empty($this->filter['$and']) || !is_array($this->filter['$and'])) {
                $this->filter['$and'] = array();
            }
            array_push(
                $this->filter['$and'], array(
                    $this->iterationField => $issetValuesArr
                )
            );
            array_push(
                $this->filter['$and'], array(
                    $this->iterationField => [$iterationDirection => $lastIterationFieldValue]
                )
            );
        } else {
            $this->filter[$this->iterationField] = array(
                $iterationDirection => $lastIterationFieldValue
            );
        }
    }

    /**
     *
     * Check incoming data for start batching and throw exceptions when data is bad
     *
     * @param Callable $callbackFunction
     */
    protected function ensureExecuteEnvironment($callbackFunction)
    {
        if (!is_callable($callbackFunction)) {
            throw new RuntimeException(__FUNCTION__, 'callback function is not callable');
        }
        if (empty($this->dbName)) {
            throw new UnexpectedValueException('dbName', $this->dbName);
        }
        if (empty($this->collectionName)) {
            throw new UnexpectedValueException('collectionName', $this->collectionName);
        }
        if(empty($this->iterationField)){
            throw new UnexpectedValueException('iterationField', $this->iterationField);
        }
        if(empty($this->iterationSort)){
            throw new UnexpectedValueException('iterationSort', $this->iterationSort);
        }
    }
}