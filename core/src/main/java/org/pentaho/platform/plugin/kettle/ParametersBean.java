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
/**
 * The Hitachi Vantara proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Hitachi Vantara. 
 */
package org.pentaho.platform.plugin.kettle;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.util.xml.dom4j.XmlDom4JHelper;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ParametersBean {

  private Log log = LogFactory.getLog( ParametersBean.class );

  private static final String PARAMETER_CORE_NAMESPACE =
      "http://reporting.pentaho.org/namespaces/engine/parameter-attributes/core";

  private Map<String, String> userParams;
  private Map<String, String> variables;
  private Document document;
  private Map<String, String> requestParams = new HashMap<String, String>();

  public ParametersBean( Map<String, String> userParams ) {
    this.userParams = userParams;
  }

  public ParametersBean( Map<String, String> userParams, Map<String, String> variables, Map<String, String> requestParams ) {
    this.userParams = userParams;
    this.variables = variables;
    this.requestParams = requestParams != null ? requestParams : new HashMap<String, String>();
  }

  public String getParametersXmlString() throws ParserConfigurationException, TransformerException, IOException {

    document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element parameters = document.createElement( "parameters" );
    parameters = setRootLevelSystemAttributes( parameters );

    // access the ktr/kjb file and identify if there are any user input parameters
    parameters = createUserParameters( parameters , requestParams );
    parameters = createUserVariables( parameters, requestParams );
    parameters = createSystemRequiredParameters( parameters );

    document.appendChild( parameters );
    StringBuffer buffer = XmlDom4JHelper.docToString( document );
    return buffer.toString();
  }

  private Element setRootLevelSystemAttributes( Element rootElement ) throws DOMException {

    rootElement.setAttribute( "autoSubmit", "true" );
    rootElement.setAttribute( "autoSubmitUI", "true" );
    rootElement.setAttribute( "accepted-page", "-1" );
    rootElement.setAttribute( "is-mandatory", "false" );
    rootElement.setAttribute( "is-prompt-needed", "false" );

    return rootElement;
  }

  private Element createAttribute( String name, String value ) {

    Element attribute = document.createElement( "attribute" ); // NON-NLS
    attribute.setAttribute( "name", name ); // NON-NLS
    attribute.setAttribute( "namespace", PARAMETER_CORE_NAMESPACE ); // NON-NLS
    attribute.setAttribute( "value", value ); // NON-NLS

    return attribute;
  }

  private Element createBaseElement( String elementName, String name, String type, boolean hidden ) {

    String defaultType = "java.lang.String"; //$NON-NLS-1$

    Element element = document.createElement( elementName ); //$NON-NLS-1$

    element.setAttribute( "is-list", "false" ); //$NON-NLS-1$ //$NON-NLS-2$
    element.setAttribute( "is-mandatory", "false" ); //$NON-NLS-1$ //$NON-NLS-2$
    element.setAttribute( "is-multi-select", "false" ); //$NON-NLS-1$ //$NON-NLS-2$
    element.setAttribute( "is-strict", "false" ); //$NON-NLS-1$ //$NON-NLS-2$
    element.setAttribute( "name", name ); //$NON-NLS-1$
    element.setAttribute( "type", StringUtils.isEmpty( type ) ? defaultType : type ); //$NON-NLS-1$

    if( hidden ) {
      element.appendChild( createAttribute( "hidden", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    return element;
  }

  private Element createUserParameters( Element parameters, Map<String, String> paramMap ) {

    try {

      if ( userParams != null ) {

        for ( Map.Entry<String, String> e : userParams.entrySet() ) {

          Element element = createBaseElement( "parameter", e.getKey(), null, false );
          element.appendChild( createAttribute( "role", "user" ) ); //$NON-NLS-1$ //$NON-NLS-2$
          element.appendChild( createAttribute( "label", e.getKey() ) ); //$NON-NLS-1$ //$NON-NLS-2$

          if ( paramMap.containsKey( e.getKey() ) ) {
            String value = paramMap.get( e.getKey() );
            element.appendChild( createValue( value, "java.lang.String", value ) );
          } else {
            element.appendChild( createValue( e.getValue(), "java.lang.String", e.getValue() ) );
          }

          parameters.appendChild( element );
        }
      }

    } catch ( Exception e ) {
      log.error( "", e );
    }

    return parameters;
  }

  private Element createUserVariables( Element parameters, Map<String, String> paramMap ) {

    try {

      if ( variables != null ) {

        for ( Map.Entry<String, String> e : variables.entrySet() ) {

          Element element = createBaseElement( "parameter", e.getKey(), null, false );
          element.appendChild( createAttribute( "role", "user" ) ); //$NON-NLS-1$ //$NON-NLS-2$
          element.appendChild( createAttribute( "label", e.getKey() ) ); //$NON-NLS-1$ //$NON-NLS-2$
          element.appendChild( createAttribute( "parameter-group", "variables" ) );
          element.appendChild( createAttribute( "parameter-group-label", "Variables" ) );

          if ( paramMap.containsKey( e.getKey() ) ) {
            String value = paramMap.get( e.getKey() );
            element.appendChild( createValue( value, "java.lang.String", value ) );
          } else {
            element.appendChild( createValue( e.getValue(), "java.lang.String", e.getValue() ) );
          }

          parameters.appendChild( element );
        }
      }

    } catch ( Exception e ) {
      log.error( "", e );
    }

    return parameters;
  }

  private Element createValue( String name, String type, String value ) {

    Element valuesElem = document.createElement( "values" ); // NON-NLS

    Element valueAttr = document.createElement( "value" ); // NON-NLS
    valueAttr.setAttribute( "label", name ); // NON-NLS
    valueAttr.setAttribute( "type", type ); // NON-NLS
    valueAttr.setAttribute( "selected", "true" ); // NON-NLS
    valueAttr.setAttribute( "null", "false" ); // NON-NLS
    valueAttr.setAttribute( "value", value ); // NON-NLS

    valuesElem.appendChild( valueAttr );

    return valuesElem;
  }

  private Element createSystemRequiredParameters( Element parameters ) {

    try {

      // parameter 'accepted-page' is needed in pentaho-prompting.js ( if it doesn't exist we'll get a undefined error )
      Element e1 = createBaseElement( "parameter", "accepted-page", "java.lang.Integer", true );
      e1.appendChild( createAttribute( "role", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e1.appendChild( createAttribute( "hidden", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e1.appendChild( createAttribute( "preferred", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e1.appendChild( createAttribute( "label", "accepted-page" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e1.appendChild( createAttribute( "parameter-group", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e1.appendChild( createAttribute( "parameter-group-label", "System Parameters" ) ); //$NON-NLS-1$ //$NON-NLS-2$

      parameters.appendChild( e1 );

      // parameter 'autoSubmit' used to disable automatic submit ( when filling out from )
      Element e2 = createBaseElement( "parameter", "autoSubmit", "java.lang.Boolean", true );
      e2.appendChild( createAttribute( "role", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createAttribute( "preferred", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createAttribute( "label", "autoSubmit" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createAttribute( "parameter-group", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createAttribute( "parameter-group-label", "System Parameters" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createAttribute( "parameter-render-type", "textbox" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createAttribute( "deprecated", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e2.appendChild( createValue( "true", "java.lang.Boolean", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

      // parameter 'autoSubmitUI' used to disable automatic submit ( when filling out from )
      Element e3 = createBaseElement( "parameter", "autoSubmitUI", "java.lang.Boolean", true );
      e3.appendChild( createAttribute( "role", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createAttribute( "preferred", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createAttribute( "label", "autoSubmitUI" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createAttribute( "parameter-group", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createAttribute( "parameter-group-label", "System Parameters" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createAttribute( "parameter-render-type", "textbox" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createAttribute( "deprecated", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e3.appendChild( createValue( "true", "java.lang.Boolean", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

      Element e4 = createBaseElement( "parameter", "showParameters", "java.lang.Boolean", true );
      e4.appendChild( createAttribute( "role", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createAttribute( "preferred", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createAttribute( "label", "autoSubmitUI" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createAttribute( "parameter-group", "system" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createAttribute( "parameter-group-label", "System Parameters" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createAttribute( "parameter-render-type", "textbox" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createAttribute( "deprecated", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      e4.appendChild( createValue( "true", "java.lang.Boolean", "true" ) ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$



      parameters.appendChild( e2 );
      parameters.appendChild( e3 );
      parameters.appendChild( e4 );



    } catch ( Exception e ) {
      log.error( "", e );
    }
    return parameters;
  }

}
