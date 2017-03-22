<?php

namespace MongoBatch;

use MongoBatch\Exception\InvalidArgumentException;
use MongoBatch\Exception\RuntimeException;
use MongoBatch\Exception\UnexpectedValueException;
use Psr\SimpleCache\CacheInterface;

/**
 *
 * MongoBatch tool execute long iterations in mongoDB collections with large data sets.
 * Also this tool can save last iteration field value in special cache server (using class interface, which implementing
 * \Psr\SimpleCache\CacheInterface) and uses this value for run from last document in subsequent launches
 *
 * Tool uses native mongoDB mechanism batchSize
 * @see https://docs.mongodb.com/manual/reference/method/cursor.batchSize/
 *
 * Class MongoBatch
 * @package MongoBatch
 * @author Dmitriy Dryutskiy emeno@yandex.ru
 */
class MongoBatch
{

    /** @var \MongoCollection $mongoCollection */
    protected $mongoCollection;

    /** @var CacheInterface $cacheClient */
    protected $cacheClient = null;

    /** @var int $iterationField */
    protected $iterationField;

    /** @var string|int $iterationSort */
    protected $iterationSort;

    /** @var string $iterationCondition */
    protected $iterationCondition;

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
    protected $limit = 0;

    /** @var bool $calcCount */
    protected $calcCount = true;

    /** @var bool $clearKeyAfter */
    protected $clearKeyAfter = false;

    /**
     * MongoBatch constructor.
     * @param \MongoCollection $_mongoCollection
     * @param CacheInterface $_cacheClient
     */
    public function __construct(\MongoCollection $_mongoCollection, CacheInterface $_cacheClient = null)
    {
        $this->mongoCollection = $_mongoCollection;
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

        $this->iterationCondition = $this->iterationSort == 1 ? '$gt' : '$lt';

        return $this;
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
     * @param bool $_clearKeyAfter
     */
    public function setClearKeyAfter($_clearKeyAfter)
    {
        $this->clearKeyAfter = (bool)$_clearKeyAfter;
    }

    /**
     * @return bool
     */
    public function getClearKeyAfter()
    {
        return $this->clearKeyAfter;
    }

    /**
     * @return string
     */
    public function getCollectionName()
    {
        return $this->mongoCollection->getName();
    }

    /**
     * @param $callbackFunction
     * @return int
     */
    public function execute($callbackFunction)
    {

        $this->ensureExecuteEnvironment($callbackFunction);

        if($this->clearIterationCache){
            $this->clearLastIterationValue();
        }

        $resultFilter = $this->getPreparedFilter();

        $cursor = $this->mongoCollection
            ->find($resultFilter, $this->fields)
            ->immortal(true)
            ->sort([$this->iterationField => $this->iterationSort]);

        if ($this->limit) {
            $cursor->limit($this->getLimit());
        }

        $queryResultsCount = -1;

        if($this->getCalcCount()){
            $queryResultsCount = $cursor->count();
        }

        $documentCounter = 0;

        while($data = $cursor->getNext()){

            $this->ensureData($data);

            $documentCounter++;

            $this->invokeCallback($callbackFunction, $data, $documentCounter, $queryResultsCount);

            if(
                $queryResultsCount > 0 &&
                $documentCounter >= $queryResultsCount
            ){
                break;
            }

            $this->saveLastIterationValue($data[$this->iterationField]);

            $this->doPause($documentCounter);
        }

        if($this->clearKeyAfter) {
            $this->clearLastIterationValue();
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
     * @return array
     */
    protected function getPreparedFilter()
    {

        $resultFilter = $this->filter;

        $lastIterationValue = $this->getLastIterationValue();

        if($lastIterationValue) {

            $synonym = $this->iterationCondition == '$gt' ? '$gte' : '$lte';

            if(isset($resultFilter[$this->iterationField])) {
                unset($resultFilter[$this->iterationField][$synonym]);
            }

            $resultFilter[$this->iterationField][$this->iterationCondition] = $lastIterationValue;

        }

        return $resultFilter;
    }

    /**
     * @return mixed|null
     */
    protected function getLastIterationValue()
    {
        if(!$this->saveState || !$this->cacheClient){
            return null;
        }

        return $this->cacheClient->get($this->prepareCacheKey(), null);
    }

    /**
     * @param $documentCounter
     */
    protected function doPause($documentCounter)
    {
        if(($documentCounter % $this->batchSize) == 0 && $this->pause > 0.00){
            usleep($this->pause * 1000000);
        }
    }

    /**
     * @param $callbackFunction
     * @param array $data
     * @param int $documentCounter
     * @param int $queryResultsCount
     */
    protected function invokeCallback($callbackFunction, $data, $documentCounter, $queryResultsCount)
    {
        call_user_func_array($callbackFunction, array(
                $data,
                $documentCounter,
                $queryResultsCount
            )
        );
    }

    /**
     * @param mixed $value
     */
    protected function saveLastIterationValue($value)
    {
        if($this->saveState && $this->cacheClient){
            $this->cacheClient->set($this->prepareCacheKey(), $value, $this->saveStateSeconds);
        }
    }

    protected function clearLastIterationValue()
    {
        if($this->cacheClient){
            $this->cacheClient->delete($this->prepareCacheKey());
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

        if(empty($this->iterationField)){
            throw new UnexpectedValueException('iterationField', $this->iterationField);
        }
        if(empty($this->iterationSort)){
            throw new UnexpectedValueException('iterationSort', $this->iterationSort);
        }
    }

    /**
     * @param array $data
     */
    protected function ensureData(array $data)
    {
        if(
            !isset($data[$this->iterationField]) ||
            empty($data[$this->iterationField])
        ){
            throw new RuntimeException(__FUNCTION__, "data[{$this->iterationField}] cannot be empty");
        }
    }
}