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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.database.service.IDatabaseDialectService;
import org.pentaho.di.core.database.DataSourceProviderInterface;
import org.pentaho.platform.api.data.IPooledDatasourceService;
import org.pentaho.platform.api.engine.IPentahoSession;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import javax.sql.DataSource;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class PlatformKettleDataSourceProviderTest {

  @Test
  public void testInvalidateNamedDataSource() throws Exception {
    try ( MockedStatic<PentahoSystem> pentahoSystemMockedStatic = Mockito.mockStatic( PentahoSystem.class ) ) {
      IPooledDatasourceService service = mock( IPooledDatasourceService.class );
      IDatabaseDialectService mockDatabaseDialectService = mock( IDatabaseDialectService.class );
      pentahoSystemMockedStatic.when(
        () -> PentahoSystem.get( eq( IPooledDatasourceService.class ), nullable( IPentahoSession.class ) ) ).thenReturn( service );
      pentahoSystemMockedStatic.when( () -> PentahoSystem.get( eq( IDatabaseDialectService.class ) ) ).thenReturn( mockDatabaseDialectService );
      DataSourceProviderInterface dsp = mock( PlatformKettleDataSourceProvider.class );

      String namedDataSource = UUID.randomUUID().toString();

      // Mock objects
      DataSource dataSource = mock( DataSource.class );
      when( service.getDataSource( namedDataSource ) ).thenReturn( dataSource );
      when( dsp.invalidateNamedDataSource( namedDataSource, DataSourceProviderInterface.DatasourceType.POOLED ) )
        .thenCallRealMethod();

      dsp.invalidateNamedDataSource( namedDataSource, DataSourceProviderInterface.DatasourceType.POOLED );
      Mockito.verify( service, Mockito.times( 1 ) ).clearDataSource( namedDataSource );
    }
  }
}
