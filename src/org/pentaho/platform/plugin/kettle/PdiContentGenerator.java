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
* Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/

package org.pentaho.platform.plugin.kettle;

import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.engine.services.messages.Messages;
import org.pentaho.platform.util.messages.LocaleHelper;
import org.pentaho.platform.web.http.api.resources.FileResourceContentGenerator;

public class PdiContentGenerator extends FileResourceContentGenerator {
  private static final Log logger = LogFactory.getLog( PdiContentGenerator.class );
  private OutputStream out;
  private RepositoryFile repositoryFile;
  private PdiAction pdiComponent;
  private StringBuffer outputStringBuffer;

  public String getMimeType( String streamPropertyName ) {
    return "text/html";
  }

  public PdiContentGenerator() {
    pdiComponent = new PdiAction();
    outputStringBuffer = new StringBuffer();
  }

  public void execute() throws Exception {

    // Test
    pdiComponent.setDirectory( FilenameUtils.getPathNoEndSeparator( repositoryFile.getPath() ) );

    // see if we are running a transformation or job
    if ( repositoryFile.getName().toLowerCase().endsWith( ".ktr" ) ) { //$NON-NLS-1$
      pdiComponent.setTransformation( FilenameUtils.getBaseName( repositoryFile.getPath() ) );
    } else if ( repositoryFile.getName().toLowerCase().endsWith( ".kjb" ) ) { //$NON-NLS-1$
      pdiComponent.setJob( FilenameUtils.getBaseName( repositoryFile.getPath() ) );
    }

    try {
      // now execute
      pdiComponent.execute();
    } catch ( Exception ex ) {
      clearOutputBuffer();
      throw ex;
    }

    // Verify if the transformation prepareExecution failed, as this exception is logged
    // and not thrown back
    if ( pdiComponent.isTransPrepareExecutionFailed() ) {
      clearOutputBuffer();
      throw new Exception( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0011_TRANSFORMATION_PREPARATION_FAILED" ) );
    }

    /**
     * Earlier after the execution is completed, code would display the content of LoggingBuffer. But the logs in the
     * LoggingBuffer is not limited to the specified transformation that is being executed and can contains the logging
     * for other transformations that are being executed by other users. To resolve this, the code is modified to
     * display the string "Action Successful" when transformation is executed successfully and display a generic error
     * page in case of exception. The detailed logging will continue to go to the log file
     */
    outputStringBuffer = formatSuccessMessage();
    // write the log to the output stream
    out.write( outputStringBuffer.toString().getBytes() );

  }

  /**
   * Added for unit testing
   *
   * @return PdiAction
   */
  protected void setPdiAction( PdiAction action ) {
    pdiComponent = action;
  }

  public void setOutputStream( OutputStream out ) {
    this.out = out;
  }

  public OutputStream getOutputStream() {
    return out;
  }

  public RepositoryFile getRepositoryFile() {
    return repositoryFile;
  }

  public void setRepositoryFile( RepositoryFile repositoryFile ) {
    this.repositoryFile = repositoryFile;
  }

  protected StringBuffer formatSuccessMessage() {
    StringBuffer messageBuffer = new StringBuffer();
    messageBuffer.append( "<html><head><title>" ) //$NON-NLS-1$
        .append( Messages.getInstance().getString( "MessageFormatter.USER_START_ACTION" ) ) //$NON-NLS-1$
        .append(
            "</title><link rel=\"stylesheet\" type=\"text/css\" href=\"/pentaho-style/active/default.css\"></head>" ) //$NON-NLS-1$
        .append( "<body dir=\"" ).append( LocaleHelper.getTextDirection() ).append( //$NON-NLS-1$
            "\"><table cellspacing=\"10\"><tr><td class=\"portlet-section\" colspan=\"3\">" ) //$NON-NLS-1$
        .append( Messages.getInstance().getString( "MessageFormatter.USER_ACTION_SUCCESSFUL" ) ) //$NON-NLS-1$
        .append( "<hr size=\"1\"/></td></tr><tr><td class=\"portlet-font\" valign=\"top\">" ); //$NON-NLS-1$

    return messageBuffer;

  }

  private void clearOutputBuffer() {
    if ( outputStringBuffer != null ) {
      outputStringBuffer.setLength( 0 );
    }
  }

  /**
   * Added for unit testing
   *
   * @return
   */
  protected StringBuffer getOutputStringBuffer() {
    return outputStringBuffer;
  }
}
