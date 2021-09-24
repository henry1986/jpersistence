package org.daiv

import kotlinx.coroutines.runBlocking

fun runTest(block: suspend () -> Unit) = runBlocking{ block()}
