<?php

namespace MongoBatch\Exception;

/**
 * Class InvalidArgumentException
 * @package MongoBatch\Exception
 * @author Dmitriy Dryutskiy emeno@yandex.ru
 */
class InvalidArgumentException extends \InvalidArgumentException
{

    /** @var string $argumentName */
    protected $argumentName;

    /** @var mixed $argumentValue */
    protected $argumentValue;

    /**
     * InvalidArgumentException constructor.
     * @param string $_argumentName
     * @param int $_argumentValue
     * @param \Exception|null $_previous
     */
    public function __construct($_argumentName, $_argumentValue, \Exception $_previous = null)
    {

        $this->argumentName = $_argumentName;
        $this->argumentValue = $_argumentValue;

        $message = 'Invalid argument '.$_argumentName.' = '.(string)$_argumentValue;

        parent::__construct($message, 0, $_previous);
    }

    /**
     * @return string
     */
    public function getArgumentName()
    {
        return $this->argumentName;
    }

    /**
     * @return mixed
     */
    public function getArgumentValue()
    {
        return $this->argumentValue;
    }
}