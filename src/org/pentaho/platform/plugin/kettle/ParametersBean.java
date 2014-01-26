/**
 * The Pentaho proprietary code is licensed under the terms and conditions
 * of the software license agreement entered into between the entity licensing
 * such code and Pentaho Corporation. 
 */
package org.pentaho.platform.plugin.kettle;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.util.xml.dom4j.XmlDom4JHelper;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ParametersBean {

  private Log log = LogFactory.getLog( ParametersBean.class );

  private static final String PARAMETER_CORE_NAMESPACE =
      "http://pentaho.pdi.plugin/namespaces/engine/parameter-attributes/core";

  private String[] userParams;
  private Document document;

  public ParametersBean( String[] userParams ) {
    this.userParams = userParams;
  }

  public String getParametersXmlString() throws ParserConfigurationException, TransformerException, IOException {

    document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element parameters = document.createElement( "parameters" );
    parameters = setRootLevelSystemAttributes( parameters );

    // access the ktr/kjb file and identify if there are any user input parameters
    parameters = createUserParameters( parameters );

    document.appendChild( parameters );
    StringBuffer buffer = XmlDom4JHelper.docToString( document );
    return buffer.toString();
  }

  private Element setRootLevelSystemAttributes( Element rootElement ) throws DOMException {

    rootElement.setAttribute( "autoSubmit", Boolean.toString( false ) );
    rootElement.setAttribute( "autoSubmitUI", Boolean.toString( false ) );
    rootElement.setAttribute( "is-prompt-needed", Boolean.toString( false ) );
    rootElement.setAttribute( "accepted-page", "-1" );
    rootElement.setAttribute( "page-count", "0" );

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
    element.setAttribute( "autoSubmit", Boolean.toString( false ) ); //$NON-NLS-1$
    element.setAttribute( "autoSubmitUI", Boolean.toString( false ) ); //$NON-NLS-1$
    element.setAttribute( "is-prompt-needed", Boolean.toString( false ) ); //$NON-NLS-1$
    element.setAttribute( "name", name ); //$NON-NLS-1$
    element.setAttribute( "type", StringUtils.isEmpty( type ) ? defaultType : type ); //$NON-NLS-1$

    element.appendChild( createAttribute( "autoSubmit", Boolean.toString( false ) ) ); //$NON-NLS-1$
    element.appendChild( createAttribute( "autoSubmitUI", Boolean.toString( false ) ) ); //$NON-NLS-1$
    element.appendChild( createAttribute( "is-prompt-needed", Boolean.toString( false ) ) ); //$NON-NLS-1$
    element.appendChild( createAttribute( "hidden", Boolean.toString( hidden ) ) ); //$NON-NLS-1$

    return element;
  }

  private Element createUserParameters( Element parameters ) {

    try {

      if ( userParams != null ) {

        for ( String param : userParams ) {

          Element element = createBaseElement( "parameter", param, null, true );
          element.appendChild( createAttribute( "hidden", "false" ) ); //$NON-NLS-1$ //$NON-NLS-2$
          element.appendChild( createAttribute( "label", param ) ); //$NON-NLS-1$ //$NON-NLS-2$

          parameters.appendChild( element );
        }
      }

    } catch ( Exception e ) {
      log.error( "", e );
    }

    return parameters;
  }

}
