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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.api.engine.IParameterProvider;
import org.pentaho.platform.api.engine.IPluginManager;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.util.IPdiContentProvider;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.services.solution.SimpleContentGenerator;
import org.pentaho.util.messages.LocaleHelper;

import java.io.OutputStream;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ParameterContentGenerator returns the available parameters for a given ktr/kjb in XML format
 */
public class ParameterContentGenerator extends SimpleContentGenerator {

  private static final long serialVersionUID = -5766894670107979596L;

  private static final String PATH = "path"; //$NON-NLS-1$
  private static final String FILE = "file"; //$NON-NLS-1$

  private Log log = LogFactory.getLog( ParameterContentGenerator.class );

  @Override
  public void createContent( OutputStream out ) throws Exception {

    IParameterProvider pathParams = parameterProviders.get( PATH );
    IParameterProvider requestParams = parameterProviders.get( IParameterProvider.SCOPE_REQUEST );

    RepositoryFile file = null;

    if ( pathParams != null ) {

      file = (RepositoryFile) pathParams.getParameter( FILE );

    } else {

      IUnifiedRepository repo = PentahoSystem.get( IUnifiedRepository.class, null );

      String path = URLDecoder.decode( requestParams.getStringParameter( PATH, StringUtils.EMPTY ), LocaleHelper.UTF_8 );
      file = repo.getFile( idTopath( path ) );
    }

    IPdiContentProvider provider =
        (IPdiContentProvider) PentahoSystem.get( IPluginManager.class ).getBean(
            IPdiContentProvider.class.getSimpleName() );

    Map<String, String> userParams = provider.getUserParameters( file.getPath() );
    Map<String, String> userVariables = provider.getVariables( file.getPath() );

    // Ultimately, user variables come from inspecting strings in the transmeta and matching on "${" and "}"
    // this means that if we have a param defined and have a reference it somewhere in the ktr, it will show up
    // as both a param and a variable -- we only want the param to show up in that case.
    Map<String, String> filteredUserVariables = userVariables.entrySet().stream()
      .filter( i -> !userParams.containsKey( i.getKey() ) )
      .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ));

    ParametersBean paramBean = new ParametersBean( userParams, filteredUserVariables, requestParameterToStringMap( requestParams ) );
    String response = paramBean.getParametersXmlString();

    out.write( response.getBytes( LocaleHelper.getSystemEncoding() ) );
    out.flush();
  }

  @Override
  public String getMimeType() {
    return "text/xml";
  }

  @Override
  public Log getLogger() {
    return log;
  }

  protected String idTopath( String id ) {
    String path = id.replace( ":", "/" );
    if ( path != null && path.length() > 0 && path.charAt( 0 ) != '/' ) {
      path = "/" + path;
    }
    return path;
  }

  private Map<String, String> requestParameterToStringMap( IParameterProvider requestParams ){

    Map<String, String> paramMap = new HashMap<>();

    if( requestParams != null ){

      Iterator<String> it = requestParams.getParameterNames();

      while( it.hasNext() ){

        String name = it.next();
        String value = "";
        if ( requestParams.hasParameter( name ) ) {
          Object paramVal = requestParams.getParameter( name );
          if ( paramVal instanceof String[] ) {
            // jobs scheduled through PUC appear to have all of their parameters duplicated in the data stored, which leads
            // to these request params being a list of strings instead of a single string.
            String[] paramArray = (String[])paramVal;
            value = paramArray.length > 0 ? paramArray[ 0 ] : "";
          } else {
            value = requestParams.getParameter( name ).toString();
          }
        }
        paramMap.put( name, value );
      }
    }

    return paramMap;
  }
}
