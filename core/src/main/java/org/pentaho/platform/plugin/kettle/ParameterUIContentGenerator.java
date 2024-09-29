/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package org.pentaho.platform.plugin.kettle;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.pentaho.platform.api.engine.IPluginManager;
import org.pentaho.platform.api.engine.IPluginResourceLoader;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.services.solution.SimpleContentGenerator;
import org.pentaho.platform.util.messages.LocaleHelper;

public class ParameterUIContentGenerator extends SimpleContentGenerator {

  private static final long serialVersionUID = -6537706461351621253L;

  String viewerFilePath;
  String pluginId;
  Map<String, String> resourceMap;

  @Override
  public Log getLogger() {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void createContent( OutputStream outputStream ) throws Exception {

    IPluginResourceLoader resourceLoader = PentahoSystem.get( IPluginResourceLoader.class );
    IPluginManager pluginManager = PentahoSystem.get( IPluginManager.class );
    ClassLoader classLoader = pluginManager.getClassLoader( pluginId );
    String filePath = !viewerFilePath.startsWith( "/" ) ? "/" + viewerFilePath : viewerFilePath;

    String viewer =
        IOUtils
            .toString( resourceLoader.getResourceAsStream( classLoader, filePath ), LocaleHelper.getSystemEncoding() );

    viewer = doResourceReplacement( viewer );

    InputStream is = IOUtils.toInputStream( viewer, LocaleHelper.getSystemEncoding() );

    IOUtils.copy( is, outputStream );
    outputStream.flush();
  }

  @Override
  public String getMimeType() {
    return "text/html";
  }

  public String getPluginId() {
    return pluginId;
  }

  public void setPluginId( String pluginId ) {
    this.pluginId = pluginId;
  }

  public Map<String, String> getResourceMap() {
    return resourceMap;
  }

  public void setResourceMap( Map<String, String> resourceMap ) {
    this.resourceMap = resourceMap;
  }

  public String getViewerFilePath() {
    return viewerFilePath;
  }

  public void setViewerFilePath( String viewerFilePath ) {
    this.viewerFilePath = viewerFilePath;
  }

  public String doResourceReplacement( String viewer ) {

    if ( !StringUtils.isEmpty( viewer ) && resourceMap != null && resourceMap.keySet() != null ) {

      Iterator it = resourceMap.keySet().iterator();

      while ( it.hasNext() ) {

        String resource = (String)it.next();

        if ( !StringUtils.isEmpty( resource ) && viewer.contains( resource ) ) {
          viewer = viewer.replaceAll( resource, resourceMap.get( resource ) );
        }
      }
    }

    return viewer;
  }
}
