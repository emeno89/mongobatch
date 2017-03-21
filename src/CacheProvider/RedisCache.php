<?php

namespace MongoBatch\CacheProvider;

use Predis\Client;
use Psr\SimpleCache\CacheInterface;

/**
 * Class RedisCache
 * @package MongoBatch\CacheProvider
 * @author Dmitriy Dryutskiy
 */
class RedisCache implements CacheInterface
{

    protected $redisClient;

    /**
     * RedisCache constructor.
     * @param Client $_redisClient
     */
    public function __construct(Client $_redisClient)
    {
        $this->redisClient = $_redisClient;
    }

    /**
     * @param string $key
     * @param mixed $default
     * @return null|string
     */
    public function get($key, $default = null)
    {
        $value = $this->redisClient->get($key);

        if(empty($value)){
            return $default;
        }

        return $value;
    }

    /**
     * @param string $key
     * @param mixed $value
     * @param int $ttl
     * @return bool
     */
    public function set($key, $value, $ttl = null)
    {
        return (bool)$this->redisClient->set($key, $value, $ttl);
    }

    /**
     * @param string $key
     * @return bool
     */
    public function delete($key)
    {
        return (bool)$this->redisClient->del([$key]);
    }

    /**
     * @param array $keys
     * @return int
     */
    public function deleteMultiple($keys)
    {
        return (bool)$this->redisClient->del((array)$keys);
    }

    /**
     * @param array $keys
     * @param mixed $default
     * @return []
     */
    public function getMultiple($keys, $default = null)
    {

        $values = [];

        foreach($keys as $key){
            $value = $this->redisClient->get($key);
            if(empty($value)){
                $value = $default;
            }
            $values[] = $value;
        }

        return $values;
    }

    /**
     * @param array $values
     * @param int $ttl
     */
    public function setMultiple($values, $ttl = null)
    {
        foreach($values as $key => $value){
            $this->redisClient->set($key, $value, $ttl);
        }

        return true;
    }

    public function has($key)
    {
        return $this->redisClient->exists($key);
    }

    public function clear()
    {
        // TODO: Implement clear() method.
    }
}