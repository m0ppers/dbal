<?php
class Connection extends \Doctrine\DBAL\Driver\Connection
{
    protected $_drizzle;
    protected $_dbh;

    public function __construct($host, $port, $db, $username, $password)
    {
        $this->_drizzle = drizzle_create();
        $this->_dbh = drizzle_con_add_tcp($this->drizzle, $host, $port, $username, $password, $db, 0);
    }

    public function prepare($prepareString)
    {
    }
}
