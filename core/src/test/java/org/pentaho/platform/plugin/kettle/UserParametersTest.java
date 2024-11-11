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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.api.engine.ISolutionEngine;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.util.IPdiContentProvider;
import org.pentaho.platform.engine.core.system.PathBasedSystemSettings;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.core.system.boot.PlatformInitializationException;
import org.pentaho.platform.engine.security.SecurityHelper;
import org.pentaho.platform.engine.services.solution.SolutionEngine;
import org.pentaho.platform.repository2.unified.fs.FileSystemBackedUnifiedRepository;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserParametersTest {

  // this is the parameter named that should be filtered out, given that it starts with an underscore
  private static final String SAMPLE_PROTECTED_PARAMETER_NAME = "_protected";

  private static final String SOLUTION_REPOSITORY = "target/test-classes/solution";
  private static final String SAMPLE_TRANS = "/org/pentaho/platform/plugin/kettle/UserParametersTest.ktr";
  private static final String SAMPLE_JOB = "/org/pentaho/platform/plugin/kettle/UserParametersTest.kjb";

  private MicroPlatform mp = new MicroPlatform( SOLUTION_REPOSITORY );

  public static final String SESSION_PRINCIPAL = "SECURITY_PRINCIPAL";

  private String TEST_USER = "TestUser";

  @Before
  public void init() throws PlatformInitializationException {
    System.setProperty( "java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory" );
    System.setProperty( "org.osjava.sj.root", "test-src/simple-jndi" );
    System.setProperty( "org.osjava.sj.delimiter", "/" );
    System.setProperty( "PENTAHO_SYS_CFG_PATH", new File( SOLUTION_REPOSITORY + "/pentaho.xml" ).getAbsolutePath() );

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession( session );

    mp.define( IUserRoleListService.class, StubUserRoleListService.class );
    mp.define( UserDetailsService.class, StubUserDetailService.class );
    mp.defineInstance( IAuthorizationPolicy.class, new TestAuthorizationPolicy() );
    mp.setSettingsProvider( new PathBasedSystemSettings() );
    mp.define( ISolutionEngine.class, SolutionEngine.class );
    FileSystemBackedUnifiedRepository repo =  new FileSystemBackedUnifiedRepository( SOLUTION_REPOSITORY );
    mp.defineInstance(  IUnifiedRepository.class, repo );

    mp.start();

    SecurityHelper.getInstance().becomeUser( TEST_USER );
  }

  @Test
  public void testTransformationUserParameters() throws Exception {

    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setDirectory( SOLUTION_REPOSITORY );
    action.setTransformation( SAMPLE_TRANS );

    action.execute();

    Assert.isTrue( action.localTrans != null && action.localTrans.getTransMeta() != null );

    NamedParams np = action.localTrans.getTransMeta();

    boolean protectedParameterNameExistsInKtr = false;

    for ( String param : np.listParameters() ) {
      protectedParameterNameExistsInKtr |= param != null && param.equals( SAMPLE_PROTECTED_PARAMETER_NAME );
    }

    // we make sure: this ktr does indeed have a system/hidden parameter
    Assert.isTrue( protectedParameterNameExistsInKtr );

    // reset attribute
    protectedParameterNameExistsInKtr = false;

    // we now call IPdiContentProvider.getUserParameters( kjb ), that should filter out protected parameters
    IPdiContentProvider pdiContentProvider = new PdiContentProvider( PentahoSystem.get( IUnifiedRepository.class ) );
    Map<String, String> userParams = pdiContentProvider.getUserParameters( SOLUTION_REPOSITORY + SAMPLE_TRANS + ".ktr" );

    for ( String userParam : userParams.keySet() ) {
      protectedParameterNameExistsInKtr |= userParam != null && userParam.equals( SAMPLE_PROTECTED_PARAMETER_NAME );
    }

    // we make sure: IPdiContentProvider has filtered it out
    Assert.isTrue( !protectedParameterNameExistsInKtr );
  }

  @Test
  public void testJobUserParameters() throws Exception {

    PdiAction action = new PdiAction();
    action.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    action.setArguments( new String[] { "dummyArg" } );
    action.setDirectory( SOLUTION_REPOSITORY );
    action.setJob( SAMPLE_JOB );

    action.execute();

    Assert.isTrue( action.localJob != null && action.localJob.getJobMeta() != null );

    NamedParams np = action.localJob.getJobMeta();

    boolean protectedParameterNameExistsInKjb = false;

    for ( String param : np.listParameters() ) {
      protectedParameterNameExistsInKjb |= param != null && param.equals( SAMPLE_PROTECTED_PARAMETER_NAME );
    }

    // we make sure: this kjb does indeed have a system/hidden parameter
    Assert.isTrue( protectedParameterNameExistsInKjb );

    // reset attribute
    protectedParameterNameExistsInKjb = false;

    // we now call IPdiContentProvider.getUserParameters( kjb ), that should filter out protected parameters
    IPdiContentProvider pdiContentProvider = new PdiContentProvider( PentahoSystem.get( IUnifiedRepository.class ) );
    Map<String, String> userParams = pdiContentProvider.getUserParameters( SOLUTION_REPOSITORY + SAMPLE_JOB + ".kjb" );

    for ( String userParam : userParams.keySet() ) {
      protectedParameterNameExistsInKjb |= userParam != null && userParam.equals( SAMPLE_PROTECTED_PARAMETER_NAME );
    }

    // we make sure: IPdiContentProvider has filtered it out
    Assert.isTrue( !protectedParameterNameExistsInKjb );
  }

  @After
  public void tearDown() {
    if ( mp != null ) {
      mp.stop();
    }
  }

  public class TestAuthorizationPolicy implements IAuthorizationPolicy {

    List<String> allowedActions = new ArrayList<String>();

    @Override
    public List<String> getAllowedActions( String arg0 ) {
      allowedActions.add( "org.pentaho.repository.read" );
      allowedActions.add( "org.pentaho.repository.create" );
      return allowedActions;
    }

    @Override
    public boolean isAllowed( String arg0 ) {
      return true;
    }

  }

}
