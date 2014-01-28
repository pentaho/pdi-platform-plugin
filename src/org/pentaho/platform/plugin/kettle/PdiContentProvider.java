package org.pentaho.platform.plugin.kettle;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.NamedParamsDefault;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.util.IPdiContentProvider;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;

public class PdiContentProvider implements IPdiContentProvider {

  private Log log = LogFactory.getLog( PdiContentProvider.class );

  IUnifiedRepository unifiedRepository;

  public PdiContentProvider() {
    this.unifiedRepository = PentahoSystem.get( IUnifiedRepository.class, PentahoSessionHolder.getSession() );
  }
  
  public PdiContentProvider( IUnifiedRepository unifiedRepository ) {
    this.unifiedRepository = unifiedRepository;
  }

  @Override
  public boolean hasUserParameters( String kettleFilePath ) {

    String[] userParams = getUserParameters( kettleFilePath );
    return userParams != null && userParams.length > 0;
  }

  @Override
  public String[] getUserParameters( String kettleFilePath ) {

    List<String> userParams = new ArrayList<String>();

    if ( !StringUtils.isEmpty( kettleFilePath ) ) {

      try {

        NamedParams np = getMeta( kettleFilePath );

        if ( !isEmpty( np = filterUserParameters( np ) ) ) {

          return np.listParameters();
        }

      } catch ( KettleException e ) {
        log.error( e );
      }
    }

    return userParams.toArray( new String[] {} );
  }

  private NamedParams filterUserParameters( NamedParams params ) {

    NamedParams userParams = new NamedParamsDefault();

    if ( !isEmpty( params ) ) {

      for ( String paramName : params.listParameters() ) {

        if ( isUserParameter( paramName ) ) {
          try {
            userParams.addParameterDefinition( paramName, StringUtils.EMPTY, StringUtils.EMPTY );
          } catch ( DuplicateParamException e ) {
            // ignore
          }
        }
      }
    }

    return params;
  }

  private NamedParams getMeta( String kettleFilePath ) throws KettleException {

    NamedParams meta = null;

    if ( !StringUtils.isEmpty( kettleFilePath ) ) {

      String extension = FilenameUtils.getExtension( kettleFilePath );
      if ( "ktr".equalsIgnoreCase( extension ) ) {

        meta = new TransMeta( kettleFilePath );

      } else if ( "kjb".equalsIgnoreCase( extension ) ) {

        meta = new JobMeta( kettleFilePath, null );

      }
    }

    return meta;
  }

  private boolean isUserParameter( String paramName ) {

    if ( !StringUtils.isEmpty( paramName ) ) {
      // TODO: add logic to filter user parameters
    }
    return true; // for now..
  }

  private boolean isEmpty( NamedParams np ) {
    return np == null || np.listParameters() == null || np.listParameters().length == 0;

  }
}
