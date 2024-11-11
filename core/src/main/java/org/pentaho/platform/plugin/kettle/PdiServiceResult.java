/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
