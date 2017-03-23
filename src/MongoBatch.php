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
     *
     * @param string $_iterationField
     * @param int|string $_iterationSort
     * @return $this
     * @throws \MongoBatch\Exception\InvalidArgumentException
     */
    public function setIterationField($_iterationField, $_iterationSort = 1)
    {
        $this->iterationField = $_iterationField;

        if (is_int($_iterationSort)) {
            $this->iterationSort = $_iterationSort == 1 ? 1 : -1;
        } else if (is_string($_iterationSort)) {
            $this->iterationSort = $_iterationSort == 'asc' ? 1 : -1;
        } else {
            throw new InvalidArgumentException('iterationSort', $_iterationSort);
        }

        $this->iterationCondition = $this->iterationSort == 1 ? '$gt' : '$lt';

        return $this;
    }

    /**
     * @param array $_filter
     * @return $this
     */
    public function setFilter(array $_filter)
    {
        $this->filter = $_filter;
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
     * @param array $_fields
     * @return $this
     */
    public function setFields(array $_fields)
    {
        $this->fields = $_fields;
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
     * @param int $_batchSize
     * @return $this
     * @throws \MongoBatch\Exception\UnexpectedValueException
     */
    public function setBatchSize($_batchSize)
    {
        $this->batchSize = (int)$_batchSize;

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
     * @param $_saveState
     * @return $this
     */
    public function setSaveState($_saveState)
    {
        $this->saveState = (bool)$_saveState;
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
     * @param float $_pause
     * @return $this
     */
    public function setPause($_pause)
    {
        $this->pause = (float)$_pause;
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
     * @param $_seconds
     * @return $this
     */
    public function setSaveStateSeconds($_seconds)
    {
        $this->saveStateSeconds = (int)$_seconds;
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
     * @param int $_limit
     * @return $this
     * @throws \MongoBatch\Exception\UnexpectedValueException
     */
    public function setLimit($_limit)
    {
        $this->limit = (int)$_limit;

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
     * @param bool $_clearIterationCache
     * @return $this
     */
    public function setClearIterationCache($_clearIterationCache)
    {
        $this->clearIterationCache = (bool)$_clearIterationCache;
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
     * @param bool $_calcCount
     * @return $this
     */
    public function setCalcCount($_calcCount)
    {
        $this->calcCount = (bool)$_calcCount;
        return $this;
    }

    /**
     * @param bool $_clearKeyAfter
     * @return $this
     */
    public function setClearKeyAfter($_clearKeyAfter)
    {
        $this->clearKeyAfter = (bool)$_clearKeyAfter;

        return $this;
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

            $this->saveLastIterationValue($data[$this->iterationField]);

            if(
                $queryResultsCount > 0 &&
                $documentCounter >= $queryResultsCount
            ){
                break;
            }

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