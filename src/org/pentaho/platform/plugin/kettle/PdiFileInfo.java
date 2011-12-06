package org.pentaho.platform.plugin.kettle;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.engine.IFileInfo;
import org.pentaho.platform.api.engine.ISolutionFile;
import org.pentaho.platform.api.engine.ISolutionFileMetaProvider;
import org.pentaho.platform.engine.core.solution.FileInfo;
import org.pentaho.platform.plugin.action.messages.Messages;
import org.pentaho.platform.util.xml.w3c.XmlW3CHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Parses a PDI file and creates an IFileInfo object containing the
 * job or transformation envelope information
 * @author jamesdixon
 *
 */
    
public class PdiFileInfo implements ISolutionFileMetaProvider {

  private Log logger = LogFactory.getLog(PdiFileInfo.class);
  
  /**
   * Parses a PDI job or transformation file and returns an IFileInfo object
   * containing the name, description, and author.
   */
  @Override
  public IFileInfo getFileInfo(ISolutionFile solutionFile, InputStream in) {
    try {
      
      String filename = solutionFile.getFileName();

      //FIXME: don't assume UTF-8
      String xml = IOUtils.toString(in, "UTF-8");

      // parse the document
      Document doc = XmlW3CHelper.getDomFromString( xml );
      Node root = doc.getFirstChild();
      
      if( filename.toLowerCase().endsWith( ".ktr" ) ) { //$NON-NLS-1$
        // handle a transformation
        // create a TransMeta from the document
        TransMeta transMeta = new TransMeta( root, null );
        // get the information we need
        IFileInfo info = new FileInfo();
        info.setAuthor( transMeta.getCreatedUser() );
        info.setDescription( transMeta.getDescription() );
        info.setDisplayType( "pdi-transformation" ); //$NON-NLS-1$
        info.setIcon( null );
        info.setTitle( transMeta.getName() );
        // return the IFileInfo object
        return info;
      }
      if( filename.toLowerCase().endsWith( ".kjb" ) ) { //$NON-NLS-1$
        // handle a transformation
        // create a JobMeta from the document
        JobMeta jobMeta = new JobMeta(root, null, null );
        // get the information we need
        IFileInfo info = new FileInfo();
        info.setAuthor( jobMeta.getCreatedUser() );
        info.setDescription( jobMeta.getDescription() );
        info.setDisplayType( "pdi-job" ); //$NON-NLS-1$
        info.setIcon( null );
        info.setTitle( jobMeta.getName() );
        // return the IFileInfo object
        return info;
      }
      
      return null;
    } catch (Exception e) {
          logger.error( Messages.getInstance().getErrorString("PdiFileInfo.ERROR_0001_PARSING_DOCUMENT", solutionFile.getFullPath()), e ); //$NON-NLS-1
      }
      return null;
  }

}
