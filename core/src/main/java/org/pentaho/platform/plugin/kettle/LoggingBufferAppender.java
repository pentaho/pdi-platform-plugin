/*!
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package org.pentaho.platform.plugin.kettle;

import org.apache.commons.lang.NotImplementedException;
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
    throw new NotImplementedException();
  }

  @Override
  public boolean ignoreExceptions() {
    return false;
  }

  @Override
  public ErrorHandler getHandler() {
    throw new NotImplementedException();
  }

  @Override
  public void setHandler(ErrorHandler handler) {
    throw new NotImplementedException();
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
