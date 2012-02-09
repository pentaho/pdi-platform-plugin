/*
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
 * Copyright 2006 - 2012 Pentaho Corporation.  All rights reserved.
 *
 */
package org.pentaho.platform.plugin.kettle;

/**
 * This class is a compound object that describes the result of executing
 * a PDI transformation or job
 * @author jamesdixon
 *
 */
public class PdiServiceResult {

  private String status;
  
  private int result;
  
  private String log;

  /**
   * Returns the status message of the execution
   * @return
   */
  public String getStatus() {
    return status;
  }

  /**
   * Sets the status message of the execution
   * @param status
   */
  public void setStatus(String status) {
    this.status = status;
  }

  /**
   * Returns the status code of the execution
   * @return
   */
  public int getResult() {
    return result;
  }

  /**
   * Sets the status code of the execution
   * @param result
   */
  public void setResult(int result) {
    this.result = result;
  }

  /**
   * Returns the log of the execution
   * @return
   */
  public String getLog() {
    return log;
  }

  /**
   * Sets the log of the execution
   * @param log
   */
  public void setLog(String log) {
    this.log = log;
  }
  
  
  
}
