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
* Copyright (c) 2002-2017 Pentaho Corporation..  All rights reserved.
*/

package org.pentaho.platform.plugin.kettle;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.pentaho.di.core.logging.DefaultLogLevel;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LoggingBuffer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class LoggingBufferAppender implements Appender {
  private final LoggingBuffer loggingBuffer;
  
  public LoggingBufferAppender(LoggingBuffer loggingBuffer) {
    this.loggingBuffer = loggingBuffer;
  }

  @Override
  public void addFilter(Filter arg0) {
    throw new NotImplementedException();
  }

  @Override
  public void clearFilters() {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
    throw new NotImplementedException();
  }

  @Override
  public void doAppend(LoggingEvent event) {
    KettleLoggingEvent kle = new KettleLoggingEvent(event.getMessage(), System.currentTimeMillis(), DefaultLogLevel.getLogLevel());
    loggingBuffer.doAppend(kle);
  }

  @Override
  public ErrorHandler getErrorHandler() {
    throw new NotImplementedException();
  }

  @Override
  public Filter getFilter() {
    throw new NotImplementedException();
  }

  @Override
  public Layout getLayout() {
    throw new NotImplementedException();
  }

  @Override
  public String getName() {
    return loggingBuffer.getName();
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  public void setErrorHandler(ErrorHandler errorHandler) {
    throw new NotImplementedException();
  }

  @Override
  public void setLayout(Layout layout) {
    throw new NotImplementedException();
  }

  @Override
  public void setName(String name) {
    loggingBuffer.setName(name);
  }
}
