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
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.pentaho.platform.plugin.kettle;

import java.io.FileNotFoundException;
import java.text.MessageFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.platform.plugin.kettle.messages.Messages;

public class EngineMetaLoader {

  private static final Log log = LogFactory.getLog( EngineMetaLoader.class );

  private Repository repository;

  public EngineMetaLoader( Repository repository ) {
    this.repository = repository;
  }

  public TransMeta loadTransMeta( String directory, String filename ) throws FileNotFoundException {
    if ( directory == null || filename == null ) {
      throw new IllegalArgumentException( Messages.getInstance().getErrorString(
          "EngineMetaLoader.ERROR_0001_DIR_OR_FILENAME_NULL" ) ); //$NON-NLS-1$
    }

    return load( directory, filename, TransMeta.class );
  }

  public JobMeta loadJobMeta( String directory, String filename ) throws FileNotFoundException {
    if ( directory == null || filename == null ) {
      throw new IllegalArgumentException( Messages.getInstance().getErrorString(
          "EngineMetaLoader.ERROR_0001_DIR_OR_FILENAME_NULL" ) ); //$NON-NLS-1$
    }

    return load( directory, filename, JobMeta.class );
  }

  /**
   * Loads a transformation from the PDI repository
   * 
   * @param directoryName
   * @param transformationName
   * @return
   * @throws FileNotFoundException
   */
  @SuppressWarnings( "unchecked" )
  private <T> T load( final String directoryName, final String fileName, Class<T> metaType )
    throws FileNotFoundException {

    if ( log.isDebugEnabled() ) {
      log.debug( MessageFormat.format( "attempting to load dir: {0} file: {1} from repository: {2}", directoryName, //$NON-NLS-1$
          fileName, ( repository == null ) ? "" : repository.getName() ) ); //$NON-NLS-1$
    }

    T meta = null;
    try {
      if ( repository != null ) {
        // Load the transformation from the repository the "new way"
        RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree().findDirectory( directoryName );
        if ( metaType == TransMeta.class ) {
          meta = (T) repository.loadTransformation( fileName, directory, null, true, null );
        } else {
          meta = (T) repository.loadJob( fileName, directory, null, null );
        }
      } else {
        // temporary debug for testing failure cases on mac
        if ( log.isDebugEnabled() ) {
          log.debug( "Repository is null!" ); //$NON-NLS-1$
          dumpRepositoryNames();
        }
      }
    } catch ( Exception e ) {
      log.error( MessageFormat.format(
          "Using the new Repository API, we could not find dir: {0} file: {1} in repository: {2}", directoryName, //$NON-NLS-1$
          fileName, ( repository == null ) ? "" : repository.getName() ), e ); //$NON-NLS-1$
    }

    if ( meta == null ) {
      // Load the job or trans the old way. If repository is null, that is ok, the meta object will attempt to load from
      // the FS
      try {
        if ( metaType == TransMeta.class ) {
          meta = (T) new TransMeta( directoryName + "/" + fileName, repository ); //$NON-NLS-1$
        } else {
          meta = (T) new JobMeta( directoryName + "/" + fileName, repository ); //$NON-NLS-1$
        }
      } catch ( KettleException e ) {
        log.error( MessageFormat.format(
            "Using the old Repository API, failed to open file: {0}/{1} in repository: {2}", directoryName, //$NON-NLS-1$
            fileName, ( repository == null ) ? "" : repository.getName() ), e ); //$NON-NLS-1$
      }
    }

    if ( meta == null ) {
      String message =
          MessageFormat.format( Messages.getInstance()
              .getErrorString( "EngineMetaLoader.ERROR_0002_PDI_FILE_NOT_FOUND" ), //$NON-NLS-1$
              directoryName, fileName, ( repository == null ) ? "" : repository.getName() ); //$NON-NLS-1$ 
      throw new FileNotFoundException( message );
    }
    return meta;
  }

  private void dumpRepositoryNames() {
    try {
      RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
      repositoriesMeta.readData(); // Read from the default $HOME/.kettle/repositories.xml file.
      for ( int i = 0; i < repositoriesMeta.nrRepositories(); i++ ) {
        RepositoryMeta repoMeta = repositoriesMeta.getRepository( i );
        log.debug( "Found repo: " + repoMeta.getName() + " type: " + repoMeta.getClass().getName() ); //$NON-NLS-1$//$NON-NLS-2$
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }
}
