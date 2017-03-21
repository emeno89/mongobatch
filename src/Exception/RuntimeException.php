<?php

namespace MongoBatch\Exception;

/**
 * Class RuntimeException
 * @package MongoBatch\Exception
 * @author Dmitriy Dryutskiy emeno@yandex.ru
 */
class RuntimeException extends \RuntimeException
{

    /**
     * RuntimeException constructor.
     * @param string $_method
     * @param string $_message
     * @param \Exception|null $_previous
     */
    public function __construct($_method, $_message = "", \Exception $_previous = null)
    {
        $resultMessage = $_method.': '.$_message;
        parent::__construct($resultMessage, 0, $_previous);
    }
}