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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.platform.api.engine.IParameterProvider;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.engine.IPluginManager;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.util.IPdiContentProvider;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.test.platform.engine.core.MicroPlatform;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ParameterContentGenerator class
 */
public class ParameterContentGeneratorTest {

  private static final String SOLUTION_REPOSITORY = "target/test-classes/solution";

  private MicroPlatform mp;
  private ParameterContentGenerator generator;
  private IParameterProvider pathParams;
  private IParameterProvider requestParams;
  private IPdiContentProvider pdiContentProvider;
  private IPluginManager pluginManager;
  private IUnifiedRepository repository;
  private OutputStream outputStream;

  @Before
  public void setUp() throws Exception {
    mp = new MicroPlatform( SOLUTION_REPOSITORY );

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession( session );

    // Initialize mocks
    pathParams = mock( IParameterProvider.class );
    requestParams = mock( IParameterProvider.class );
    pdiContentProvider = mock( IPdiContentProvider.class );
    pluginManager = mock( IPluginManager.class );
    repository = mock( IUnifiedRepository.class );
    outputStream = new ByteArrayOutputStream();

    // Setup PentahoSystem mocks
    when( pluginManager.getBean( IPdiContentProvider.class.getSimpleName() ) ).thenReturn( pdiContentProvider );
    mp.defineInstance( IPluginManager.class, pluginManager );
    mp.defineInstance( IUnifiedRepository.class, repository );

    mp.start();

    // Create generator instance
    generator = new ParameterContentGenerator();
  }

  @After
  public void tearDown() {
    if ( mp != null ) {
      mp.stop();
    }
  }

  @Test
  public void testCreateContent_WithPathParams_RepositoryFile() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );

    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    userParams.put( "param1", "value1" );
    userParams.put( "param2", "value2" );

    Map<String, String> variables = new HashMap<>();
    variables.put( "var1", "varValue1" );
    variables.put( "param1", "duplicate" ); // This should be filtered out

    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock empty request parameters
    when( requestParams.getParameterNames() ).thenReturn( new java.util.ArrayList<String>().iterator() );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );
    assertTrue( output.contains( "<parameters" ) );
    assertTrue( output.contains( "param1" ) );
    assertTrue( output.contains( "param2" ) );
    assertTrue( output.contains( "var1" ) );

    verify( pdiContentProvider ).getUserParameters( "/path/to/file.ktr" );
    verify( pdiContentProvider ).getVariables( "/path/to/file.ktr" );
  }

  @Test
  public void testCreateContent_WithPathParams_FileObject() throws Exception {
    // Setup
    FileObject fileObject = mock( FileObject.class );
    FileName fileName = mock( FileName.class );
    when( fileObject.getName() ).thenReturn( fileName );
    when( fileName.getPath() ).thenReturn( "/vfs/path/to/file.ktr" );

    when( pathParams.getParameter( "file" ) ).thenReturn( fileObject );

    Map<String, String> userParams = new HashMap<>();
    userParams.put( "param1", "value1" );

    Map<String, String> variables = new HashMap<>();
    variables.put( "var1", "varValue1" );

    when( pdiContentProvider.getUserParameters( "/vfs/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/vfs/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock empty request parameters
    when( requestParams.getParameterNames() ).thenReturn( new java.util.ArrayList<String>().iterator() );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );
    assertTrue( output.contains( "<parameters" ) );

    verify( pdiContentProvider ).getUserParameters( fileObject );
    verify( pdiContentProvider ).getVariables( fileObject );
  }

  @Test
  public void testCreateContent_WithoutPathParams_UsingRepository() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/home/admin/file.ktr" );

    when( requestParams.getStringParameter( eq( "path" ), anyString() ) ).thenReturn( "home:admin:file.ktr" );
    when( repository.getFile( "/home/admin/file.ktr" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    userParams.put( "param1", "value1" );

    Map<String, String> variables = new HashMap<>();
    variables.put( "var1", "varValue1" );

    when( pdiContentProvider.getUserParameters( "/home/admin/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/home/admin/file.ktr" ) ).thenReturn( variables );

    // Mock empty request parameters
    when( requestParams.getParameterNames() ).thenReturn( new java.util.ArrayList<String>().iterator() );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );

    verify( repository ).getFile( "/home/admin/file.ktr" );
    verify( pdiContentProvider ).getUserParameters( "/home/admin/file.ktr" );
  }

  @Test
  public void testCreateContent_WithNullFile() throws Exception {
    // Setup
    when( pathParams.getParameter( "file" ) ).thenReturn( null );

    Map<String, String> userParams = new HashMap<>();
    Map<String, String> variables = new HashMap<>();

    // Mock empty request parameters
    when( requestParams.getParameterNames() ).thenReturn( new java.util.ArrayList<String>().iterator() );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify - should still produce output despite null file
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );
    assertTrue( output.contains( "<parameters" ) );

    verify( pdiContentProvider, never() ).getUserParameters( any( String.class ) );
  }

  @Test
  public void testCreateContent_WithUnexpectedFileType() throws Exception {
    // Setup - use an unexpected object type
    Object unexpectedFile = new String( "unexpected" );
    when( pathParams.getParameter( "file" ) ).thenReturn( unexpectedFile );

    // Mock empty request parameters
    when( requestParams.getParameterNames() ).thenReturn( new java.util.ArrayList<String>().iterator() );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify - should still produce output
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );

    verify( pdiContentProvider, never() ).getUserParameters( any( String.class ) );
  }

  @Test
  public void testCreateContent_WithRequestParameters_StringValue() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );
    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    Map<String, String> variables = new HashMap<>();
    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock request parameters with string values
    Iterator<String> paramNames = new java.util.ArrayList<String>(
        java.util.Arrays.asList( "reqParam1", "reqParam2" )
    ).iterator();
    when( requestParams.getParameterNames() ).thenReturn( paramNames );
    when( requestParams.hasParameter( "reqParam1" ) ).thenReturn( true );
    when( requestParams.getParameter( "reqParam1" ) ).thenReturn( "reqValue1" );
    when( requestParams.hasParameter( "reqParam2" ) ).thenReturn( true );
    when( requestParams.getParameter( "reqParam2" ) ).thenReturn( "reqValue2" );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );
  }

  @Test
  public void testCreateContent_WithRequestParameters_StringArrayValue() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );
    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    Map<String, String> variables = new HashMap<>();
    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock request parameters with string array values (simulating scheduled jobs)
    Iterator<String> paramNames = new java.util.ArrayList<String>(
        java.util.Arrays.asList( "arrayParam" )
    ).iterator();
    when( requestParams.getParameterNames() ).thenReturn( paramNames );
    when( requestParams.hasParameter( "arrayParam" ) ).thenReturn( true );
    when( requestParams.getParameter( "arrayParam" ) ).thenReturn( new String[] { "firstValue", "secondValue" } );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );
  }

  @Test
  public void testCreateContent_WithRequestParameters_EmptyStringArray() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );
    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    Map<String, String> variables = new HashMap<>();
    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock request parameters with empty string array
    Iterator<String> paramNames = new java.util.ArrayList<String>(
        java.util.Arrays.asList( "emptyArrayParam" )
    ).iterator();
    when( requestParams.getParameterNames() ).thenReturn( paramNames );
    when( requestParams.hasParameter( "emptyArrayParam" ) ).thenReturn( true );
    when( requestParams.getParameter( "emptyArrayParam" ) ).thenReturn( new String[] {} );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
  }

  @Test
  public void testCreateContent_WithRequestParameters_NoValue() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );
    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    Map<String, String> variables = new HashMap<>();
    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock request parameters with no value
    Iterator<String> paramNames = new java.util.ArrayList<String>(
        java.util.Arrays.asList( "noValueParam" )
    ).iterator();
    when( requestParams.getParameterNames() ).thenReturn( paramNames );
    when( requestParams.hasParameter( "noValueParam" ) ).thenReturn( false );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
  }

  @Test
  public void testCreateContent_WithNullRequestParams() throws Exception {
    // Setup
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );
    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    Map<String, String> variables = new HashMap<>();
    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, null );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify - should still work with null request params
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "<?xml version=" ) );
  }

  @Test
  public void testGetMimeType() {
    // Execute
    String mimeType = generator.getMimeType();

    // Verify
    assertEquals( "text/xml", mimeType );
  }

  @Test
  public void testIdToPath_WithColonSeparator() {
    // Execute
    String result = generator.idTopath( "home:admin:file.ktr" );

    // Verify
    assertEquals( "/home/admin/file.ktr", result );
  }

  @Test
  public void testIdToPath_WithLeadingSlash() {
    // Execute
    String result = generator.idTopath( "/home/admin/file.ktr" );

    // Verify
    assertEquals( "/home/admin/file.ktr", result );
  }

  @Test
  public void testIdToPath_WithoutLeadingSlash() {
    // Execute
    String result = generator.idTopath( "home/admin/file.ktr" );

    // Verify
    assertEquals( "/home/admin/file.ktr", result );
  }

  @Test
  public void testIdToPath_EmptyString() {
    // Execute
    String result = generator.idTopath( "" );

    // Verify
    assertEquals( "", result );
  }

  @Test
  public void testIdToPath_SingleColon() {
    // Execute
    String result = generator.idTopath( "home" );

    // Verify
    assertEquals( "/home", result );
  }

  @Test
  public void testCreateContent_VariableFilteredByParameter() throws Exception {
    // Setup - test that variables with same name as params are filtered out
    RepositoryFile repositoryFile = mock( RepositoryFile.class );
    when( repositoryFile.getPath() ).thenReturn( "/path/to/file.ktr" );
    when( pathParams.getParameter( "file" ) ).thenReturn( repositoryFile );

    Map<String, String> userParams = new HashMap<>();
    userParams.put( "param1", "paramValue" );
    userParams.put( "param2", "paramValue2" );

    Map<String, String> variables = new HashMap<>();
    variables.put( "param1", "duplicateValue" ); // Should be filtered
    variables.put( "var1", "varValue" ); // Should remain
    variables.put( "param2", "duplicateValue2" ); // Should be filtered

    when( pdiContentProvider.getUserParameters( "/path/to/file.ktr" ) ).thenReturn( userParams );
    when( pdiContentProvider.getVariables( "/path/to/file.ktr" ) ).thenReturn( variables );

    // Mock empty request parameters
    when( requestParams.getParameterNames() ).thenReturn( new java.util.ArrayList<String>().iterator() );

    Map<String, IParameterProvider> paramProviders = new HashMap<>();
    paramProviders.put( "path", pathParams );
    paramProviders.put( IParameterProvider.SCOPE_REQUEST, requestParams );

    generator.setParameterProviders( paramProviders );

    // Execute
    generator.createContent( outputStream );

    // Verify
    String output = outputStream.toString();
    assertNotNull( output );
    assertTrue( output.contains( "param1" ) );
    assertTrue( output.contains( "param2" ) );
    assertTrue( output.contains( "var1" ) );
  }
}
