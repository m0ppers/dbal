<?php
namespace Doctrine\DBAL\Driver\Drizzle;

class Connection implements \Doctrine\DBAL\Driver\Connection
{
    protected $_drizzle;
    protected $_dbh;

    private $lastStatement;

    public function __construct($host, $port, $db, $username, $password)
    {
        $this->_drizzle = drizzle_create();
        $this->_dbh = drizzle_con_add_tcp($this->_drizzle, $host, (int)$port, $username, $password, $db, 0);
    }

    public function prepare($prepareString)
    {
        $this->lastStatement = new Statement($this->_drizzle, $this->_dbh, $prepareString);
        return $this->lastStatement;
    }
    
    public function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();
        return $stmt;
    }
    
    /**
     * {@inheritdoc}
     */
    public function quote($input, $type=\PDO::PARAM_STR)
    {
        if (is_int($input) || is_float($input)) {
            return $input;
        }
        return "'". $this->_drizzle->escapeString($input) ."'";
    }
    
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();
        return $stmt->rowCount();
    }

    public function lastInsertId($name = null)
    {
        return $this->lastStatement->getResult()->insertId();
    }

    public function beginTransaction()
    {
        $this->_dbh->query('START TRANSACTION');
        return true;
    }

    public function rollBack()
    {
        $this->_dbh->query('ROLLBACK');
        return true;
    }
    
    public function commit()
    {
        $this->_dbh->query('COMMIT');
        return true;
    }

    public function errorCode()
    {
        return $this->_dbh->errorCode();
    }

    public function errorInfo()
    {
        return $this->_dbh->errorString();
    }
}
