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
* Copyright (c) 2002-2018 Hitachi Vantara..  All rights reserved.
*/

package org.pentaho.platform.plugin.kettle;

import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.engine.core.audit.AuditHelper;
import org.pentaho.platform.engine.core.audit.MessageTypes;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.services.messages.Messages;
import org.pentaho.platform.web.http.api.resources.FileResourceContentGenerator;

public class PdiContentGenerator extends FileResourceContentGenerator {

  private static final long serialVersionUID = 3654713863075785759L;
  private static final Log logger = LogFactory.getLog( PdiContentGenerator.class );
  private OutputStream out;
  private RepositoryFile repositoryFile;
  private PdiAction pdiComponent;
  private StringBuilder outputStringBuilder;

  public String getMimeType( String streamPropertyName ) {
    return "text/html";
  }

  public PdiContentGenerator() {
    pdiComponent = new PdiAction();
    outputStringBuilder = new StringBuilder();
  }

  public void execute() throws Exception {

    String pdiPath = repositoryFile.getPath();
    // Test
    pdiComponent.setDirectory( FilenameUtils.getPathNoEndSeparator( pdiPath ) );

    // see if we are running a transformation or job
    if ( repositoryFile.getName().toLowerCase().endsWith( ".ktr" ) ) { //$NON-NLS-1$
      pdiComponent.setTransformation( FilenameUtils.getBaseName( pdiPath ) );
    } else if ( repositoryFile.getName().toLowerCase().endsWith( ".kjb" ) ) { //$NON-NLS-1$
      pdiComponent.setJob( FilenameUtils.getBaseName( pdiPath ) );
    }
    IPentahoSession session = PentahoSessionHolder.getSession();
    long start = System.currentTimeMillis();
    try {
      AuditHelper.audit( session.getId(), session.getName(), pdiPath, getObjectName(), this.getClass().getName(),
          MessageTypes.INSTANCE_START, instanceId, "", 0, this ); //$NON-NLS-1$
      // now execute
      pdiComponent.execute();
      AuditHelper.audit( session.getId(), session.getName(), pdiPath, getObjectName(), this.getClass().getName(),
          MessageTypes.INSTANCE_END, instanceId, "", ( (float) ( System.currentTimeMillis() - start ) / 1000 ), this ); //$NON-NLS-1$
    } catch ( Exception ex ) {
      AuditHelper.audit( session.getId(), session.getName(), pdiPath, getObjectName(), this.getClass().getName(),
          MessageTypes.INSTANCE_FAILED, instanceId, "", ( (float) ( System.currentTimeMillis() - start ) / 1000 ), this ); // $NON-NLS-1$
      logger.error( ex );
      clearOutputBuffer();
      throw ex;
    }

    // Verify if the transformation prepareExecution failed or if there is any error in execution, as this exception is logged
    // and not thrown back
    org.pentaho.platform.plugin.kettle.messages.Messages pdiPluginMessages = org.pentaho.platform.plugin.kettle.messages.Messages.getInstance();
    if ( !pdiComponent.isExecutionSuccessful() ) {
      clearOutputBuffer();
      String errorMessage = Messages.getInstance().getErrorString( "Kettle.ERROR_0011_TRANSFORMATION_PREPARATION_FAILED" );
      AuditHelper.audit( session.getId(), session.getName(), pdiPath, getObjectName(), this.getClass().getName(),
          MessageTypes.INSTANCE_FAILED, instanceId, errorMessage,
          ( (float) ( System.currentTimeMillis() - start ) / 1000 ), this ); // $NON-NLS-1$

      String heading = pdiComponent.isTransPrepareExecutionFailed()
              ? pdiPluginMessages.getString( "PdiAction.STATUS_NOT_RUN_HEADING" ) : pdiPluginMessages.getString( "PdiAction.STATUS_ERRORS_HEADING" );
      String description = pdiComponent.isTransPrepareExecutionFailed()
              ? pdiPluginMessages.getString( "PdiAction.STATUS_NOT_RUN_DESC" ) : pdiPluginMessages.getString( "PdiAction.STATUS_ERRORS_DESC" );

      outputStringBuilder = formatMessage( "content/pdi-platform-plugin/resources/images/alert.svg", heading, description );
      out.write( outputStringBuilder.toString().getBytes() );

      return;
    }

    /**
     * Earlier after the execution is completed, code would display the content of LoggingBuffer. But the logs in the
     * LoggingBuffer is not limited to the specified transformation that is being executed and can contains the logging
     * for other transformations that are being executed by other users. To resolve this, the code is modified to
     * display the string "Action Successful" when transformation is executed successfully and display a generic error
     * page in case of exception. The detailed logging will continue to go to the log file
     */
    outputStringBuilder = formatMessage( "content/pdi-platform-plugin/resources/images/success.svg",
            pdiPluginMessages.getString( "PdiAction.STATUS_SUCCESS_HEADING" ),
            pdiPluginMessages.getString( "PdiAction.STATUS_SUCCESS_DESC" ) );
    out.write( outputStringBuilder.toString().getBytes() );
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

  protected StringBuilder formatMessage( String imgPath, String heading, String descriptions ) {
    StringBuilder messageBuilder = new StringBuilder();
    messageBuilder.append( "<html>" )
            .append( "  <base href=\"" ).append( PentahoSystem.getApplicationContext().getFullyQualifiedServerURL() ).append( "\">" )
            .append( "  <head>" )
            .append( "    <script src='js/themes.js'></script>" )
            .append( "  </head>" )
            .append( "  <body style='width: 100%; height: 100%; display: flex; justify-content: center; align-items: center; flex-direction: column'; margin: 0; padding: 0>" )
            .append( "    <div style='margin: 0 auto; width: 410px; display: flex; padding: 30px;'>" )
            .append( "      <img src='" ).append( imgPath ).append( "' style='float: left; width: 48px; height: 43px; margin-top: 3px;'/>" )
            .append( "      <div>" )
            .append( "        <div style='font-size: 25px; font-weight: normal; padding: 0px 0px 8px 13px; letter-spacing: -0.5px; height: 25px; line-height: 25px;'>" ).append( heading ).append( "</div>" )
            .append( "        <div style='padding: 0px 0px 0px 15px;'>" ).append( descriptions ).append( "</div>" )
            .append( "        <div style='padding-top: 30px; padding-left: 15px;'>" )
            .append( "          <button type='submit' class='pentaho-button' onclick='closeStatusPage()'>Close</button>" )
            .append( "        </div>" )
            .append( "      </div>" )
            .append( "    </div>" )
            .append( "  </body>" )
            .append( "  <script>" )
            .append( "    var active_theme_tree = core_theme_tree[active_theme];" )
            .append( "    document.write('<link rel=\"stylesheet\" type=\"text/css\" href=\"' + active_theme_tree.rootDir + active_theme_tree.resources[0] + '\"');" )
            .append( "    var closeStatusPage = function() {" )
            .append( "      if(window.parent.mantle_initialized) {" )
            .append( "        window.parent.closeTab('');" )
            .append( "      } else {" )
            .append( "        window.close();" )
            .append( "      }" )
            .append( "    }" )
            .append( "  </script>" )
            .append( "</html>" );

    return messageBuilder;
  }

  private void clearOutputBuffer() {
    if ( outputStringBuilder != null ) {
      outputStringBuilder.setLength( 0 );
    }
  }

  /**
   * Added for unit testing
   *
   * @return
   */
  protected StringBuilder getOutputStringBuilder() {
    return outputStringBuilder;
  }
}
