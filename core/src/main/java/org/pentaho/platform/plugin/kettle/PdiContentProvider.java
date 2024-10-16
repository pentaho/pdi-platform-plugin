package org.pentaho.platform.plugin.kettle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.Const;
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

    Map<String, String> userParams = getUserParameters( kettleFilePath );
    return userParams != null && !userParams.isEmpty();
  }

  @Override
  public Map<String, String> getUserParameters(String kettleFilePath ) {

    Map<String, String> userParams = new HashMap<>();

    if ( !StringUtils.isEmpty( kettleFilePath ) ) {
      try {
        NamedParams np = getMeta( kettleFilePath );
        if ( !isEmpty( np = filterUserParameters( np ) ) ) {
          for( String s : np.listParameters() ) {
            userParams.put(s, np.getParameterValue( s ) );
          }
        }
      } catch ( KettleException e ) {
        log.error( e );
      }
    }
    return userParams;
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

    return userParams;
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

  @Override
  public Map<String, String> getVariables( String kettleFilePath ) {
    return new HashMap<>();
  }

  private boolean isUserParameter( String paramName ) {

    if ( !StringUtils.isEmpty( paramName ) ) {
      // prevent rendering of protected/hidden/system parameters      
      if( paramName.startsWith( IPdiContentProvider.PROTECTED_PARAMETER_PREFIX ) ){
          return false;
      }
    }
    return true;
  }

  private boolean isEmpty( NamedParams np ) {
    return np == null || np.listParameters() == null || np.listParameters().length == 0;

  }

  @Override
  public String getHideInternalVariable(){
    return System.getProperty( Const.HIDE_INTERNAL_VARIABLES, Const.HIDE_INTERNAL_VARIABLES_DEFAULT );
  }
}
