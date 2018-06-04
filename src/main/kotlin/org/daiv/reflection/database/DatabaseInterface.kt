package org.daiv.reflection.database

import java.sql.Statement

interface DatabaseInterface : SimpleDatabase{
    val statement: Statement
}