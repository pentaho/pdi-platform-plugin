package org.pentaho.platform.plugin.kettle;

import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.web.http.api.resources.FileResourceContentGenerator;

public class PdiContentGenerator extends FileResourceContentGenerator {
  private static final Log logger = LogFactory.getLog(PdiContentGenerator.class);
  private OutputStream out;
  private RepositoryFile repositoryFile;

  public String getMimeType(String streamPropertyName) {
    return "text/html";
  }

  public void execute() throws Exception {
    // create the PDI component
    PdiAction pdiComponent = new PdiAction();
    
    pdiComponent.setDirectory(repositoryFile.getPath());
    
    // see if we are running a transformation or job
    if( repositoryFile.getName().toLowerCase().endsWith( ".ktr" ) ) { //$NON-NLS-1$
      pdiComponent.setTransformation(repositoryFile.getPath());
    }
    else if( repositoryFile.getName().toLowerCase().endsWith( ".kjb" ) ) { //$NON-NLS-1$
      pdiComponent.setJob(repositoryFile.getPath());
    }
    
    // create a map of the inputs
    // Map<String,Object> inputs = new HashMap<String,Object>();
    // Iterator inputNames = requestParameters.getParameterNames();
    // while( inputNames.hasNext() ) {
    // String name = (String) inputNames.next();
    // if( !name.equals( PATH ) ) {
    // inputs.put( name, requestParameters.getParameter( name ) );
    // }
    // }
    // pdiComponent.setVarArgs( inputs );

    // now execute
    pdiComponent.execute();
    
    // write the log to the output stream
    out.write( "<html><head/><body>\n".getBytes() ); //$NON-NLS-1$
    out.write( "<p/><pre>\n".getBytes() ); //$NON-NLS-1$
    out.write( pdiComponent.getLog().getBytes() );
    out.write( "</pre></body>\n".getBytes() ); //$NON-NLS-1$
  }

  public void setOutputStream(OutputStream out) {
    this.out = out;
  }

  public OutputStream getOutputStream() {
    return out;
  }

  public RepositoryFile getRepositoryFile() {
    return repositoryFile;
  }

  public void setRepositoryFile(RepositoryFile repositoryFile) {
    this.repositoryFile = repositoryFile;
  }

}
