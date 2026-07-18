/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.platform.plugin.kettle;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.LogEvent;
import org.pentaho.di.core.logging.DefaultLogLevel;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LoggingBuffer;

import java.io.Serializable;


public class LoggingBufferAppender implements Appender {
  private final LoggingBuffer loggingBuffer;

  public LoggingBufferAppender( LoggingBuffer loggingBuffer ) {
    this.loggingBuffer = loggingBuffer;
  }


  @Override
  public void append(LogEvent event) {
    KettleLoggingEvent kle = new KettleLoggingEvent( event.getMessage(), System.currentTimeMillis(), DefaultLogLevel.getLogLevel() );
    loggingBuffer.doAppend( kle );
  }

  @Override
  public String getName() {
    return loggingBuffer.getName();
  }

  @Override
  public Layout<? extends Serializable> getLayout() {
    throw new UnsupportedOperationException( "getLayout is not implemented" );
  }

  @Override
  public boolean ignoreExceptions() {
    return false;
  }

  @Override
  public ErrorHandler getHandler() {
    throw new UnsupportedOperationException( "getHandler is not implemented" );
  }

  @Override
  public void setHandler(ErrorHandler handler) {
    throw new UnsupportedOperationException( "setHandler is not implemented" );
  }

  @Override
  public State getState() {
    return null;
  }

  @Override
  public void initialize() {

  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public boolean isStarted() {
    return false;
  }

  @Override
  public boolean isStopped() {
    return false;
  }
}
