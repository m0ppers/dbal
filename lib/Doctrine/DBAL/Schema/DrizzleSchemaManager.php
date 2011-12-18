<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the LGPL. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\DBAL\Schema;

/**
 * Schema manager for the MySql RDBMS.
 *
 * @license     http://www.opensource.org/licenses/lgpl-license.php LGPL
 * @author      Konsta Vesterinen <kvesteri@cc.hut.fi>
 * @author      Lukas Smith <smith@pooteeweet.org> (PEAR MDB2 library)
 * @author      Roman Borschel <roman@code-factory.org>
 * @author      Benjamin Eberlei <kontakt@beberlei.de>
 * @version     $Revision$
 * @since       2.0
 */
class DrizzleSchemaManager extends AbstractSchemaManager
{
    protected function _getPortableViewDefinition($view)
    {
        return new View($view['TABLE_NAME'], $view['VIEW_DEFINITION']);
    }

    protected function _getPortableTableDefinition($table)
    {
        return array_shift($table);
    }

    protected function _getPortableUserDefinition($user)
    {
        return array(
            'user' => $user['User'],
            'password' => $user['Password'],
        );
    }

    protected function _getPortableTableIndexesList($tableIndexes, $tableName=null)
    {
        foreach($tableIndexes AS $k => $v) {
            $v = array_change_key_case($v, CASE_LOWER);
            if($v['key_name'] == 'PRIMARY') {
                $v['primary'] = true;
            } else {
                $v['primary'] = false;
            }
            $tableIndexes[$k] = $v;
        }
        
        return parent::_getPortableTableIndexesList($tableIndexes, $tableName);
    }

    protected function _getPortableSequenceDefinition($sequence)
    {
        return end($sequence);
    }

    protected function _getPortableDatabaseDefinition($database)
    {
        return $database['Database'];
    }
    
    /**
     * Gets a portable column definition.
     * 
     * The database type is mapped to a corresponding Doctrine mapping type.
     * 
     * @param $tableColumn
     * @return array
     */
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        $tableColumn = array_change_key_case($tableColumn, CASE_LOWER);

        $dbType = strtolower($tableColumn['type']);

        $length = $tableColumn['length'];
        $unsigned = $fixed = null;

        if ( ! isset($tableColumn['name'])) {
            $tableColumn['name'] = '';
        }
        
        $scale = null;
        $precision = null;
        
        $type = $this->_platform->getDoctrineTypeMapping($dbType);
        $type = $this->extractDoctrineTypeFromComment($tableColumn['comment'], $type);
        $tableColumn['comment'] = $this->removeDoctrineTypeFromComment($tableColumn['comment'], $type);

        switch ($dbType) {
            case 'char':
                $fixed = true;
                break;
            case 'float':
            case 'double':
            case 'real':
            case 'numeric':
            case 'decimal':
                $scale = $tableColumn['scale'];
                $precision = $tableColumn['precision'];
                break;
        }

        $length = ((int) $length == 0) ? null : (int) $length;

        $options = array(
            'length'        => $length,
            'unsigned'      => (bool)$unsigned,
            'fixed'         => (bool)$fixed,
            'default'       => $tableColumn['default'],
            'notnull'       => (bool) ($tableColumn['null'] != '1'),
            'scale'         => $scale,
            'precision'     => $precision,
            'autoincrement' => (bool) $tableColumn['auto_increment'] == 'YES',
            'comment'       => $tableColumn['comment'],
        );

        return new Column($tableColumn['field'], \Doctrine\DBAL\Types\Type::getType($type), $options);
    }

    public function _getPortableTableForeignKeyDefinition($tableForeignKey)
    {
        $tableForeignKey = array_change_key_case($tableForeignKey, CASE_LOWER);

        if (!isset($tableForeignKey['delete_rule']) || $tableForeignKey['delete_rule'] == "RESTRICT") {
            $tableForeignKey['delete_rule'] = null;
        }
        if (!isset($tableForeignKey['update_rule']) || $tableForeignKey['update_rule'] == "RESTRICT") {
            $tableForeignKey['update_rule'] = null;
        }

        foreach (array('constraint_columns','referenced_table_columns') as $k) {
            $v = $tableForeignKey[$k];
            $v = str_replace('`','',$v);
            $v = explode(',', $v);
            $tableForeignKey[$k] = $v;
        }
        
        return new ForeignKeyConstraint(
            $tableForeignKey['constraint_columns'],
            $tableForeignKey['referenced_table_name'],
            $tableForeignKey['referenced_table_columns'],
            $tableForeignKey['constraint_name'],
            array(
                'onUpdate' => $tableForeignKey['update_rule'],
                'onDelete' => $tableForeignKey['delete_rule'],
            )
        );
    }
}
