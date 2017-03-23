<?php

namespace MongoBatch\Exception;

/**
 * Class UnexpectedValueException
 * @package MongoBatch\Exception
 * @author Dmitriy Dryutskiy emeno@yandex.ru
 */
class UnexpectedValueException extends \UnexpectedValueException
{

    /** @var string $variableName */
    protected $variableName;

    /** @var mixed $variableValue */
    protected $variableValue;

    /**
     * InvalidArgumentException constructor.
     * @param string $_variableName
     * @param int $_variableValue
     * @param \Exception|null $_previous
     */
    public function __construct($_variableName, $_variableValue, \Exception $_previous = null)
    {
        $this->variableName = $_variableName;
        $this->variableValue = $_variableValue;
        $message = 'Unexpected value '.$_variableName.' = '.(string)$_variableValue;
        parent::__construct($message, 0, $_previous);
    }

    /**
     * @return string
     */
    public function getVariableName()
    {
        return $this->variableName;
    }

    /**
     * @return mixed
     */
    public function getVariableValue()
    {
        return $this->variableValue;
    }
}