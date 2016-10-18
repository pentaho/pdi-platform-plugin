/*!
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2002-2016 Pentaho Corporation..  All rights reserved.
*/

package org.pentaho.platform.plugin.kettle;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.test.platform.engine.core.MicroPlatform;
import org.pentaho.platform.api.engine.IUserRoleListService;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.ISolutionEngine;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.repository2.unified.fs.FileSystemBackedUnifiedRepository;
import org.pentaho.platform.engine.services.solution.SolutionEngine;
import org.pentaho.platform.scheduler2.quartz.QuartzScheduler;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.api.engine.IPentahoDefinableObjectFactory.Scope;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.StandaloneSession;
import org.pentaho.platform.engine.services.messages.Messages;

public class PdiContentGeneratorTest {

  private PdiContentGenerator pdiContentGenerator;
  private OutputStream outputStream;
  private RepositoryFile repositoryFile;
  private QuartzScheduler scheduler;
  private PdiAction pdiAction;

  @Before
  public void setUp() throws Exception {
    System.setProperty( "java.naming.factory.initial", "org.osjava.sj.SimpleContextFactory" ); //$NON-NLS-1$ //$NON-NLS-2$
    System.setProperty( "org.osjava.sj.root", "test-src/simple-jndi" ); //$NON-NLS-1$ //$NON-NLS-2$
    System.setProperty( "org.osjava.sj.delimiter", "/" ); //$NON-NLS-1$ //$NON-NLS-2$

    System.setProperty( "PENTAHO_SYS_CFG_PATH", new File( "test-src/solution/pentaho.xml" ).getAbsolutePath() ); //$NON-NLS-2$

    IPentahoSession session = new StandaloneSession();
    PentahoSessionHolder.setSession( session );

    pdiContentGenerator = new PdiContentGenerator();

    pdiAction = new PdiAction();
    pdiAction.setRepositoryName( KettleFileRepositoryMeta.REPOSITORY_TYPE_ID );
    pdiContentGenerator.setPdiAction( pdiAction );

    outputStream = mock( OutputStream.class );
    repositoryFile = mock( RepositoryFile.class );
    pdiContentGenerator.setOutputStream( outputStream );
    pdiContentGenerator.setRepositoryFile( repositoryFile );

    scheduler = new QuartzScheduler();
    scheduler.start();

    MicroPlatform mp = new MicroPlatform( "test-src/solution" );
    mp.define( IUserRoleListService.class, StubUserRoleListService.class );
    mp.define( UserDetailsService.class, StubUserDetailService.class );
    mp.defineInstance( IAuthorizationPolicy.class, new TestAuthorizationPolicy() );
    mp.defineInstance( IScheduler.class, scheduler );

    mp.define( ISolutionEngine.class, SolutionEngine.class );
    mp.define( IUnifiedRepository.class, FileSystemBackedUnifiedRepository.class, Scope.GLOBAL );
    FileSystemBackedUnifiedRepository repo =
        (FileSystemBackedUnifiedRepository) PentahoSystem.get( IUnifiedRepository.class );
    repo.setRootDir( new File( "test-src/solution" ) );

    mp.start();

  }

  @Test
  public void testExecuteSuccess() {
    when( repositoryFile.getPath() ).thenReturn( "test-src/solution/pdi/sample_success.ktr" );
    when( repositoryFile.getName() ).thenReturn( "sample_success.ktr" );

    try {
      pdiContentGenerator.execute();
      String output = pdiContentGenerator.getOutputStringBuffer().toString();
      assertTrue( output.contains( Messages.getInstance().getString( "MessageFormatter.USER_ACTION_SUCCESSFUL" ) ) );
    } catch ( Exception ex ) {
      // There should be no exception throws in this case
      ex.printStackTrace();
      fail( "Exception in executing transformation " + ex );
    }
  }

  @Test
  public void testExecuteFailure() {
    when( repositoryFile.getPath() ).thenReturn( "test-src/solution/pdi/samplePrepExecutionFailed.ktr" );
    when( repositoryFile.getName() ).thenReturn( "samplePrepExecutionFailed.ktr" );

    try {
      pdiContentGenerator.execute();
    } catch ( Exception ex ) {
      // This is expected as the transformation fails with at the prepareExecution
    }
  }

  public class TestAuthorizationPolicy implements IAuthorizationPolicy {

    List<String> allowedActions = new ArrayList<String>();

    @Override
    public List<String> getAllowedActions( String arg0 ) {
      // TODO Auto-generated method stub
      allowedActions.add( "org.pentaho.repository.read" );
      allowedActions.add( "org.pentaho.repository.create" );
      return allowedActions;
    }

    @Override
    public boolean isAllowed( String arg0 ) {
      // TODO Auto-generated method stub
      return true;
    }

  }

}
