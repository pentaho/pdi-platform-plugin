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

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.commons.connection.IPentahoResultSet;
import org.pentaho.commons.connection.memory.MemoryMetaData;
import org.pentaho.commons.connection.memory.MemoryResultSet;
import org.pentaho.di.ExecutionConfiguration;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.connections.vfs.provider.ConnectionFileProvider;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleMissingPluginsException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandlerCache;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobConfiguration;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.www.CarteSingleton;
import org.pentaho.platform.api.action.IAction;
import org.pentaho.platform.api.action.ILoggingAction;
import org.pentaho.platform.api.action.IVarArgsAction;
import org.pentaho.platform.api.engine.ActionExecutionException;
import org.pentaho.platform.api.engine.ActionValidationException;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.ISystemConfig;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.engine.core.system.PentahoSessionHolder;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.plugin.action.kettle.KettleSystemListener;
import org.pentaho.platform.plugin.action.messages.Messages;
import org.pentaho.platform.plugin.kettle.security.policy.rolebased.actions.RepositoryExecuteAction;
import org.pentaho.platform.util.ActionUtil;
import org.pentaho.di.repository.RepositoryConnectionUtils;

/**
 * An adaptation of KettleComponent to the lightweight PojoComponent/IAction framework
 *
 * @author jdixon, mdamour, aphillips
 */

/*
 * Legitimate outputs: EXECUTION_STATUS_OUTPUT - (execution-status) [JOB | TRANS] Returns the resultant execution status
 * 
 * EXECUTION_LOG_OUTPUT - (execution-log) [JOB | TRANS] Returns the resultant log
 * 
 * TRANSFORM_SUCCESS_OUTPUT - (transformation-written) [Requires MONITORSTEP to be defined] [TRANS] Returns a
 * "result-set" for all successful rows written (Unless error handling is not defined for the specified step, in which
 * case ALL rows are returned here)
 * 
 * TRANSFORM_ERROR_OUTPUT - (transformation-errors) [Requires MONITORSTEP to be defined] [TRANS] Returns a "result-set"
 * for all rows written that have caused an error
 * 
 * TRANSFORM_SUCCESS_COUNT_OUTPUT - (transformation-written-count) [Requires MONITORSTEP to be defined] [TRANS] Returns
 * a count of all rows returned in TRANSFORM_SUCCESS_OUTPUT
 * 
 * TRANSFORM_ERROR_COUNT_OUTPUT - (transformation-errors-count) [Requires MONITORSTEP to be defined] [TRANS] Returns a
 * count of all rows returned in TRANSFORM_ERROR_OUTPUT
 * 
 * Legitimate inputs: MONITORSTEP Takes the name of the step from which success and error rows can be detected
 * 
 * KETTLELOGLEVEL Sets the logging level to be used in the EXECUTION_LOG_OUTPUT Valid settings: basic detail error debug
 * minimal rowlevel
 */
public class PdiAction implements IAction, IVarArgsAction, ILoggingAction, RowListener {

  private static final String SINGLE_DI_SERVER_INSTANCE = "singleDiServerInstance";

  private static final String PLUGIN_CONFIGURATION_ID = "pdi-platform-plugin";
  protected static final String LOG_LEVEL_PROPERTY = "settings/log_level";
  protected static final String SAFE_MODE_PROPERTY = "settings/safe_mode";
  protected static final String GATHER_METRICS_PROPERTY = "settings/gather_metrics";

  private MemoryResultSet transformationOutputRows;

  private IPentahoResultSet injectorRows;

  private MemoryResultSet transformationOutputErrorRows;

  private int transformationOutputRowsCount;

  private int transformationOutputErrorRowsCount;

  private String directory; // the repository directory

  private String transformation; // the repository file

  private String job; // the repository file

  private String monitorStep = null;

  private String injectorStep = null;

  private Map<String, Object> varArgs = new HashMap<>();

  private InputStream inputStream;
  private boolean isVfs;

  /**
   * The name of the repository to use
   */
  private String repositoryName;

  private LoggingBuffer pdiUserAppender;

  private RowProducer rowInjector = null;
  // package-local for test reasons
  Job localJob = null;
  // package-local for test reasons
  Trans localTrans = null;

  private Log log = LogFactory.getLog( PdiAction.class );

  private Map<String, String> variables;

  private Map<String, String> parameters;

  private String[] arguments;

  private String logLevel;
  private String clearLog;
  private String runSafeMode;
  private String runClustered;
  private String gatheringMetrics;
  private String expandingRemoteJob;
  private String startCopyName;

  // When the Transformation prepare execution fails, the exception is logged and not thrown to the caller. Adding a
  // flag to indicate the success/failure of this steps
  private boolean transPrepExecutionFailure = false;

  public void setLogger( Log log ) {
    this.log = log;
  }

  /**
   * Validates that the component has everything it needs to execute a transformation or job
   *
   * @throws ActionValidationException if any information is missing
   */
  public void validate() throws ActionValidationException {
    // Check if the directory from which to load transformations or jobs is set
    if ( directory == null ) {
      throw new ActionValidationException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0001_DIR_NOT_SET" ) );
    }

    // Check if the name of the transformation/job to load is set
    if ( transformation == null && job == null ) {
      throw new ActionValidationException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0002_JOB_OR_TRANS_NOT_SET" ) );
    }

    if ( injectorStep != null && injectorRows == null ) {
      throw new ActionValidationException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0003_INJECTOR_ROWS_NOT_SET", injectorStep ) );
    }
  }

  public void setVariables( Map<String, String> variables ) {
    this.variables = variables;
  }

  public Map<String, String> getVariables() {
    return variables;
  }

  public void setParameters( Map<String, String> parameters ) {
    this.parameters = parameters;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setArguments( String[] arguments ) {
    this.arguments = arguments;
  }

  public String[] getArguments() {
    return arguments;
  }

  /**
   * Execute the specified transformation in the chosen repository.
   */
  @Override
  public void execute() throws Exception {

    // Reset the flag
    transPrepExecutionFailure = false;

    IAuthorizationPolicy authorizationPolicy =
        PentahoSystem.get( IAuthorizationPolicy.class, PentahoSessionHolder.getSession() );

    if ( !authorizationPolicy.isAllowed( RepositoryExecuteAction.NAME ) ) {
      throw new IllegalStateException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0010_NO_PERMISSION_TO_EXECUTE" ) );
    }

    if ( log.isDebugEnabled() ) {
      log.debug( Messages.getInstance().getString( "Kettle.DEBUG_START" ) );
    }

    validate();

    // initialize environment variables
    KettleSystemListener.environmentInit( PentahoSessionHolder.getSession() );

    pdiUserAppender = KettleLogStore.getAppender();
    Repository repository = connectToRepository();

    checkIfPvfs();

    try {

      if ( transformation != null ) {
        executeTransformation( repository );
      } else if ( job != null ) {
        executeJob( repository );
      }
    } finally {
      if ( repository != null ) {
        if ( log.isDebugEnabled() ) {
          log.debug( Messages.getInstance().getString( "Kettle.DEBUG_DISCONNECTING" ) );
        }
        repository.disconnect();
      }
    }

    XMLHandlerCache.getInstance().clear();
  }

  protected boolean isPvfs( String path ) {
    return StringUtils.isNotEmpty( path ) && path.startsWith( ConnectionFileProvider.ROOT_URI );
  }

  private void checkIfPvfs() {
    if ( this.parameters != null ) {
      String inputFile = (String) varArgs.get( ActionUtil.QUARTZ_STREAMPROVIDER_INPUT_FILE );
      if ( isPvfs( inputFile ) ) {
        log.info( "Input file is pvfs: " + inputFile );
        this.isVfs = true;
      }
    }
  }

  private TransMeta createTransMeta( Repository repository ) throws ActionExecutionException {
    // TODO: do we need to set a parameter on the job or trans meta called
    // ${pentaho.solutionpath} to mimic the old in-line xml replacement behavior
    // (see scm history for an illustration of this)?

    // TODO: beware of BISERVER-50

    EngineMetaLoader engineMetaUtil = new EngineMetaLoader( repository );

    try {
      return engineMetaUtil.loadTransMeta( directory, transformation );
    } catch ( FileNotFoundException e ) {
      throw new ActionExecutionException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0006_FAILED_TRANSMETA_CREATION", directory, transformation ), e );
    }
  }

  private TransMeta createTransMetaJCR( Repository repository ) throws ActionExecutionException {
    try {
      IUnifiedRepository unifiedRepository = PentahoSystem.get( IUnifiedRepository.class, null );
      RepositoryFile transFile = unifiedRepository.getFile( idToPath( transformation ) );

      return repository.loadTransformation( new StringObjectId( (String) transFile.getId() ), null );
    } catch ( Throwable e ) {
      throw new ActionExecutionException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0006_FAILED_TRANSMETA_CREATION", directory, transformation ), e );
    }
  }

  private void populateTransMeta( TransMeta transMeta ) {
    if ( arguments != null ) {
      transMeta.setArguments( arguments );
    }

    populateMeta( transMeta );
  }

  private void populateMeta( AbstractMeta aMeta ) {
    if ( clearLog != null ) {
      aMeta.setClearingLog( Boolean.parseBoolean( clearLog ) );
    }

    if ( gatheringMetrics != null ) {
      aMeta.setGatheringMetrics( Boolean.parseBoolean( gatheringMetrics ) );
    }

    if ( runSafeMode != null ) {
      aMeta.setSafeModeEnabled( Boolean.parseBoolean( runSafeMode ) );
    }

    if ( logLevel != null ) {
      aMeta.setLogLevel( LogLevel.getLogLevelForCode( logLevel ) );
    }

    populateInputs( aMeta, aMeta );

    overrideMetaValuesFromConfiguration( aMeta );
  }

  private void overrideMetaValuesFromConfiguration( AbstractMeta aMeta ) {
    Properties systemConfig = getPluginSettings();
    String metricsGatheringCfg = getPropertyAsString( systemConfig, GATHER_METRICS_PROPERTY );
    if ( metricsGatheringCfg != null ) {
      // Note that any string different from "true"/"false" will result into a false value
      boolean metricsGatheringVal = Boolean.parseBoolean( metricsGatheringCfg );
      aMeta.setGatheringMetrics( metricsGatheringVal );
      if ( log.isDebugEnabled() ) {
        // Using Boolean.toString just in case the original string differs from "true"/"false"
        log.debug( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.LOG_OVERRIDE_GATHER_METRICS", Boolean.toString( metricsGatheringVal ) ) );
      }
    }

    String safeModeCfg = getPropertyAsString( systemConfig, SAFE_MODE_PROPERTY );
    if ( safeModeCfg != null ) {
      // Note that any string different from "true"/"false" will result into a false value
      boolean safeModeVal = Boolean.parseBoolean( safeModeCfg );
      aMeta.setSafeModeEnabled( safeModeVal );
      if ( log.isDebugEnabled() ) {
        // Using Boolean.toString just in case the original string differs from "true"/"false"
        log.debug( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.LOG_OVERRIDE_SAFE_MODE", Boolean.toString( safeModeVal ) ) );
      }
    }

    String logLevelCfg = getPropertyAsString( systemConfig, LOG_LEVEL_PROPERTY );
    if ( logLevelCfg != null ) {
      // Note that any string different from the existing Levels will result into BASIC
      LogLevel logLevelVal = LogLevel.getLogLevelForCode( logLevelCfg );
      aMeta.setLogLevel( logLevelVal );
      if ( log.isDebugEnabled() ) {
        log.debug( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.LOG_OVERRIDE_LOG_LEVEL", logLevelVal.getCode() ) );
      }
    }
  }

  /**
   * Returns the configuration for this plugin as {@code Properties}. If an error occurs while loading, an empty
   * instance will be returned.
   *
   * @return an instance of <code>Properties</code> containing the plugin configuration or empty if an error occurred
   */
  @VisibleForTesting
  Properties getPluginSettings() {
    Properties properties;

    try {
      properties = PentahoSystem.get( ISystemConfig.class ).getConfiguration( PLUGIN_CONFIGURATION_ID ).getProperties();
    } catch ( IOException e ) {
      log.error( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
        .getErrorString( "PdiAction.ERROR_0011_FAILED_LOADING_CONFIGURATION", PLUGIN_CONFIGURATION_ID ), e );
      properties = new Properties();
    }

    return properties;
  }

  /**
   * Returns the value for the given property. If the property is not defined, if it is empty or only contains
   * spaces, it will return <code>null</code>.
   *
   * @param settings the configuration
   * @param propertyName the name of the property
   * @return the value for the given property or <code>null</code> if it is not defined or has no value
   */
  private String getPropertyAsString( Properties settings, String propertyName ) {
    String propertyValue = settings.getProperty( propertyName );
    if ( propertyValue != null ) {
      propertyValue = propertyValue.trim();
      if ( !propertyValue.isEmpty() ) {
        return propertyValue;
      }
    }

    return null;
  }

  private JobMeta createJobMeta( Repository repository ) throws ActionExecutionException {
    // TODO: do we need to set a parameter on the job or trans meta called
    // ${pentaho.solutionpath} to mimic the old in-line xml replacement behavior
    // (see scm history for an illustration of this)?

    // TODO: beware of BISERVER-50

    EngineMetaLoader engineMetaUtil = new EngineMetaLoader( repository );

    try {
      return engineMetaUtil.loadJobMeta( directory, job );
    } catch ( FileNotFoundException e ) {
      throw new ActionExecutionException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0007_FAILED_JOBMETA_CREATION", directory, job ), e );
    }
  }

  private JobMeta createJobMetaJCR( Repository repository ) throws ActionExecutionException {
    try {
      IUnifiedRepository unifiedRepository = PentahoSystem.get( IUnifiedRepository.class, null );
      RepositoryFile jobFile = unifiedRepository.getFile( idToPath( job ) );

      return repository.loadJob( new StringObjectId( (String) jobFile.getId() ), null );
    } catch ( Throwable e ) {
      throw new ActionExecutionException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0007_FAILED_JOBMETA_CREATION", directory, job ), e );
    }
  }

  private void populateJobMeta( JobMeta jobMeta ) {
    if ( arguments != null ) {
      jobMeta.setArguments( arguments );
    }

    populateMeta( jobMeta );
  }

  private String idToPath( String id ) {
    String path = id.replace( ':', '/' );
    if ( !path.isEmpty() && path.charAt( 0 ) != '/' ) {
      path = "/" + path;
    }
    return path;
  }

  private void populateInputs( NamedParams paramHolder, VariableSpace varSpace ) {
    if ( parameters != null ) {
      for ( Map.Entry<String, String> entry : parameters.entrySet() ) {
        try {
          paramHolder.setParameterValue( entry.getKey(), entry.getValue() );
        } catch ( UnknownParamException upe ) {
          log.warn( upe );
        }
      }
    }

    if ( variables != null ) {
      for ( Map.Entry<String, String> entry : variables.entrySet() ) {
        varSpace.setVariable( entry.getKey(), entry.getValue() );
      }
    }

    for ( Map.Entry<String, Object> entry : varArgs.entrySet() ) {
      varSpace.setVariable( entry.getKey(), ( entry.getValue() != null ) ? entry.getValue().toString() : null );
    }
  }

  protected boolean customizeTrans( Trans trans ) {
    // override this to customize the transformation before it runs
    // by default there is no transformation
    return true;
  }

  protected String getJobName( String carteObjectId ) {
    // the name returned here is going to be specific to the user + job path + name
    // if we just used the name, we're likely to clobber more often
    // return directory + "/" + job + " [" + PentahoSessionHolder.getSession().getName() + ":" + carteObjectId + "]";
    return job;
  }

  protected String getTransformationName( String carteObjectId ) {
    // the name returned here is going to be specific to the user + transformation path + name
    // if we just used the name, we're likely to clobber more often
    // return directory + "/" + transformation + " [" + PentahoSessionHolder.getSession().getName() + ":" +
    // carteObjectId + "]";
    return transformation;
  }

  protected void executeTransformation( Repository repository ) throws ActionExecutionException {
    TransMeta transMeta = null;

    if ( isVfs && log.isDebugEnabled() ) {
      log.debug( "using vfs, inputStream=" + inputStream );
    }
    if ( isVfs && this.inputStream != null ) {
      try {
        transMeta =
            new TransMeta( inputStream, repository, true, Variables.getADefaultVariableSpace(),
                ( msg, t1, t2 ) -> false );
      } catch ( KettleXMLException | KettleMissingPluginsException e ) {
        throw new ActionExecutionException( e );
      }
    } else {
      // try loading from internal repository before falling back onto kettle
      // the repository passed here is not used to load the transformation it is used
      // to populate available databases, etc in "standard" kettle fashion
      try {
        transMeta = createTransMetaJCR( repository );
      } catch ( Throwable t ) {
        // ignored
      }

      if ( transMeta == null ) {
        transMeta = createTransMeta( repository );
      }
    }
    if ( transMeta == null ) {
      throw new IllegalStateException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
        .getErrorString( "PdiAction.ERROR_0004_FAILED_TRANSMETA_CREATION" ) );
    }

    // Whichever the TransMeta we got, let's populate it
    populateTransMeta( transMeta );

    executeTransformation( transMeta );
  }

  /**
   * Executes a PDI transformation
   *
   * @param transMeta
   * @throws ActionExecutionException
   */
  protected void executeTransformation( final TransMeta transMeta )
    throws ActionExecutionException {
    localTrans = null;

    if ( transMeta != null ) {
     localTrans = getLocalTrans( transMeta );
    }

    if ( localTrans == null ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0010_BAD_TRANSFORMATION_METADATA" ) );
    }

    // OK, we have the transformation, now run it!

    if ( !customizeTrans( localTrans ) ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0028_CUSTOMIZATION_FUNCITON_FAILED" ) );
    }

    if ( log.isDebugEnabled() ) {
      log.debug( Messages.getInstance().getString( "Kettle.DEBUG_PREPARING_TRANSFORMATION" ) );
    }

    try {
      if( log.isDebugEnabled() ) {
        log.debug( MessageFormat.format("Executing with: gather metrics=[{0}], safe mode=[{1}], log level=[{2}]", transMeta.isGatheringMetrics(), transMeta.isSafeModeEnabled(), transMeta.getLogLevel().getCode() ) );
      }
      localTrans.prepareExecution( transMeta.getArguments() );
    } catch ( Exception e ) {
      transPrepExecutionFailure = true;
      // don't throw exception, because the scheduler may try to run this transformation again
      log.error( Messages.getInstance().getErrorString( "Kettle.ERROR_0011_TRANSFORMATION_PREPARATION_FAILED" ), e ); // $NON-NLS-1$
      return;
    }

    try {
      if ( log.isDebugEnabled() ) {
        log.debug( Messages.getInstance().getString( "Kettle.DEBUG_FINDING_STEP_IMPORTER" ) );
      }

      String stepName = getMonitorStepName();

      if ( stepName != null ) {
        registerAsStepListener( stepName, localTrans );
      }
    } catch ( Exception e ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0012_ROW_LISTENER_CREATE_FAILED" ), e );
    }

    try {
      if ( log.isDebugEnabled() ) {
        log.debug( Messages.getInstance().getString( "Kettle.DEBUG_FINDING_STEP_IMPORTER" ) );
      }

      if ( injectorStep != null ) {
        registerAsProducer( injectorStep, localTrans );
      }
    } catch ( Exception e ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0012_ROW_INJECTOR_CREATE_FAILED" ), e );
    }

    try {
      if ( log.isDebugEnabled() ) {
        log.debug( Messages.getInstance().getString( "Kettle.DEBUG_STARTING_TRANSFORMATION" ) );
      }
      localTrans.startThreads();
    } catch ( Exception e ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0013_TRANSFORMATION_START_FAILED" ), e );
    }

    // inject rows if necessary
    if ( injectorRows != null ) {
      // create a row meta
      try {
        if ( log.isDebugEnabled() ) {
          log.debug( Messages.getInstance().getString( "Injecting rows" ) );
        }
        RowMeta rowMeta = new RowMeta();
        RowMetaInterface rowMetaInterface = transMeta.getStepFields( injectorStep );
        rowMeta.addRowMeta( rowMetaInterface );

        // inject the rows
        Object[] row = injectorRows.next();
        while ( row != null ) {
          rowInjector.putRow( rowMeta, row );
          row = injectorRows.next();
        }
        rowInjector.finished();
      } catch ( Exception e ) {
        throw new ActionExecutionException( Messages.getInstance().getErrorString( "Row injection failed" ), e ); // $NON-NLS-1$
      }
    }

    try {
      // It's running in a separate thread to allow monitoring, etc.
      if ( log.isDebugEnabled() ) {
        log.debug( Messages.getInstance().getString( "Kettle.DEBUG_TRANSFORMATION_RUNNING" ) );
      }

      localTrans.waitUntilFinished();
      localTrans.cleanup();
    } catch ( Exception e ) {
      int transErrors = localTrans.getErrors();
      throw new ActionExecutionException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
          .getErrorString( "PdiAction.ERROR_0009_TRANSFORMATION_HAD_ERRORS", Integer.toString( transErrors ) ), e );
    }

    // Dump the Kettle log...
    if ( log.isDebugEnabled() ) {
      log.debug( pdiUserAppender.getBuffer().toString() );
    }

    // Build written row output
    if ( transformationOutputRows != null ) {
      transformationOutputRowsCount = transformationOutputRows.getRowCount();
    }

    // Build error row output
    if ( transformationOutputErrorRows != null ) {
      transformationOutputErrorRowsCount = transformationOutputErrorRows.getRowCount();
    }
  }

  /**
   * Returns a {@link Trans} instance based on the given metadata. To note that the configuration on log level, safe
   * mode and metrics gathering may be overridden by the server configurations. The information for this transformation
   * will also be added to Carte.
   *
   * @param transMeta the transformation metadata
   * @return a {@link Trans} instance pertaining to the given metadata
   *
   * @throws ActionExecutionException if anything went wrong
   * @see #getLocalJob(Repository, JobMeta)
   */
  private Trans getLocalTrans( TransMeta transMeta ) throws ActionExecutionException {
    try {
      String carteObjectId = UUID.randomUUID().toString();
      transMeta.setCarteObjectId( carteObjectId );

      Trans newLocalTrans = newTrans( transMeta );
      newLocalTrans.setArguments( transMeta.getArguments() );
      newLocalTrans.shareVariablesWith( transMeta );
      newLocalTrans.setLogLevel( transMeta.getLogLevel() );
      newLocalTrans.setSafeModeEnabled( transMeta.isSafeModeEnabled() );
      newLocalTrans.setGatheringMetrics( transMeta.isGatheringMetrics() );
      CarteSingleton.getInstance().getTransformationMap().addTransformation( getTransformationName( carteObjectId ),
          carteObjectId, newLocalTrans, new TransConfiguration( newLocalTrans.getTransMeta(),
          getTransExecutionConfiguration( transMeta ) ) );

      return newLocalTrans;
    } catch ( Exception e ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0010_BAD_TRANSFORMATION_METADATA" ), e );
    }
  }

  /**
   * Obtain an {@link ExecutionConfiguration} for a Transformation, filled with the proper configuration values.
   *
   * @see #getJobExecutionConfiguration(JobMeta)
   */
  private TransExecutionConfiguration getTransExecutionConfiguration( TransMeta transMeta ) {
    TransExecutionConfiguration executionConfiguration = newTransExecutionConfiguration();

    executionConfiguration.setLogLevel( transMeta.getLogLevel() );
    executionConfiguration.setClearingLog( transMeta.isClearingLog() );
    executionConfiguration.setSafeModeEnabled( transMeta.isSafeModeEnabled() );
    executionConfiguration.setGatheringMetrics( transMeta.isGatheringMetrics() );

    return executionConfiguration;
  }

  /**
   * Returns a {@link Job} instance based on the given metadata. To note that the configuration on log level, safe
   * mode and metrics gathering may be overridden by the server configurations. The information for this job will
   * also be added to Carte.
   *
   * @param repository the repository
   * @param jobMeta the transformation metadata
   * @return a {@link Job} instance pertaining to the given metadata
   *
   * @throws ActionExecutionException if anything went wrong
   * @see #getLocalTrans(TransMeta)
   */
  private Job getLocalJob( Repository repository, JobMeta jobMeta ) throws ActionExecutionException {
    try {
      String carteObjectId = UUID.randomUUID().toString();
      jobMeta.setCarteObjectId( carteObjectId );

      Job newLocalJob = newJob( repository, jobMeta );
      newLocalJob.setArguments( jobMeta.getArguments() );
      newLocalJob.shareVariablesWith( jobMeta );
      newLocalJob.setLogLevel( jobMeta.getLogLevel() );
      newLocalJob.setGatheringMetrics( jobMeta.isGatheringMetrics() );
      CarteSingleton.getInstance().getJobMap().addJob( getJobName( carteObjectId ), carteObjectId, newLocalJob,
        new JobConfiguration( newLocalJob.getJobMeta(), getJobExecutionConfiguration( jobMeta ) ) );

      return newLocalJob;
    } catch ( Exception e ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
        "Kettle.ERROR_0021_BAD_JOB_METADATA" ), e );
    }
  }

  /**
   * Obtain an {@link ExecutionConfiguration} for a Job, filled with the proper configuration values.
   *
   * @see #getTransExecutionConfiguration(TransMeta)
   */
  private JobExecutionConfiguration getJobExecutionConfiguration( JobMeta jobMeta ) {
    JobExecutionConfiguration executionConfiguration = newJobExecutionConfiguration();

    executionConfiguration.setLogLevel( jobMeta.getLogLevel() );
    executionConfiguration.setClearingLog( jobMeta.isClearingLog() );
    executionConfiguration.setSafeModeEnabled( jobMeta.isSafeModeEnabled() );
    executionConfiguration.setGatheringMetrics( jobMeta.isGatheringMetrics() );

    // For Jobs only
    if ( expandingRemoteJob != null ) {
      executionConfiguration.setExpandingRemoteJob( Boolean.parseBoolean( expandingRemoteJob ) );
    }
    if ( startCopyName != null ) {
      executionConfiguration.setStartCopyName( startCopyName );
    }

    return executionConfiguration;
  }

  @VisibleForTesting
  TransExecutionConfiguration newTransExecutionConfiguration() {
    return new TransExecutionConfiguration();
  }

  @VisibleForTesting
  Trans newTrans( TransMeta transMeta ) {
    return new Trans( transMeta );
  }

  @VisibleForTesting
  JobExecutionConfiguration newJobExecutionConfiguration() {
    return new JobExecutionConfiguration();
  }

  @VisibleForTesting
  Job newJob( Repository repository, JobMeta jobMeta ) {
    return new Job( repository, jobMeta );
  }

  /**
   * Registers this component as a step listener of a transformation. This allows this component to receive rows of data
   * from the transformation when it executes. These rows are made available to other components in the action sequence
   * as a result set.
   *
   * @param stepName
   * @param trans
   * @throws KettleStepException
   */
  protected void registerAsStepListener( String stepName, Trans trans ) throws KettleStepException {
    if ( trans != null ) {
      List<StepMetaDataCombi> stepList = trans.getSteps();
      // find the specified step
      for ( StepMetaDataCombi step : stepList ) {
        if ( step.stepname.equals( stepName ) ) {
          if ( log.isDebugEnabled() ) {
            log.debug( Messages.getInstance().getString( "Kettle.DEBUG_FOUND_STEP_IMPORTER" ) );
          }
          // this is the step we are looking for
          if ( log.isDebugEnabled() ) {
            log.debug( Messages.getInstance().getString( "Kettle.DEBUG_GETTING_STEP_METADATA" ) );
          }
          RowMetaInterface row = trans.getTransMeta().getStepFields( stepName );

          // create the metadata that the Pentaho result sets need
          String[] fieldNames = row.getFieldNames();
          String[][] columns = new String[1][fieldNames.length];
          for ( int column = 0; column < fieldNames.length; column++ ) {
            columns[0][column] = fieldNames[column];
          }
          if ( log.isDebugEnabled() ) {
            log.debug( Messages.getInstance().getString( "Kettle.DEBUG_CREATING_RESULTSET_METADATA" ) );
          }

          MemoryMetaData metaData = new MemoryMetaData( columns, null );
          transformationOutputRows = new MemoryResultSet( metaData );
          transformationOutputErrorRows = new MemoryResultSet( metaData );

          // add ourself as a row listener
          step.step.addRowListener( this );
          break;
        }
      }
    }
  }

  /**
   * Registers this component as a row producer in a transformation. This allows this component to inject rows into a
   * transformation when it is executed.
   *
   * @param stepName
   * @param trans the transformation
   * @return
   * @throws KettleException
   */
  protected boolean registerAsProducer( String stepName, Trans trans ) throws KettleException {
    if ( trans != null ) {
      rowInjector = trans.addRowProducer( stepName, 0 );
      return true;
    }

    return false;
  }

  protected void executeJob( Repository repository ) throws ActionExecutionException {
    JobMeta jobMeta = null;

    if ( isVfs ) {
      log.debug( "using vfs, inputStream=" + inputStream );
    }
    if ( isVfs && this.inputStream != null ) {
      try {
        jobMeta = new JobMeta( inputStream, repository, ( msg, t1, t2 ) -> false );
      } catch ( KettleXMLException e ) {
        throw new ActionExecutionException( e );
      }
    } else {

      // try loading from internal repository before falling back onto kettle
      // the repository passed here is not used to load the job it is used
      // to populate available databases, etc in "standard" kettle fashion
      try {
        jobMeta = createJobMetaJCR( repository );
      } catch ( Throwable t ) {
        // ignored
      }

      if ( jobMeta == null ) {
        jobMeta = createJobMeta( repository );
      }
    }
    if ( jobMeta == null ) {
      throw new IllegalStateException( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance()
        .getErrorString( "PdiAction.ERROR_0005_FAILED_JOBMETA_CREATION" ) );
    }

    // Whichever the JobMeta we got, let's populate it
    populateJobMeta( jobMeta );

    executeJob( jobMeta, repository );
  }

  /**
   * Executes a PDI job
   *
   * @param jobMeta
   * @param repository
   * @throws ActionExecutionException
   */
  protected void executeJob( final JobMeta jobMeta, final Repository repository )
    throws ActionExecutionException {
    localJob = null;

    if ( jobMeta != null ) {
     localJob = getLocalJob( repository, jobMeta );
    }

    if ( localJob == null ) {
      if ( log.isDebugEnabled() ) {
        log.debug( pdiUserAppender.getBuffer().toString() );
      }
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
          "Kettle.ERROR_0021_BAD_JOB_METADATA" ) );
    }

    // OK, we have the job, now run it!

    try {
      if ( log.isDebugEnabled() ) {
        log.debug( Messages.getInstance().getString( "Kettle.DEBUG_STARTING_JOB" ) );
      }

      if ( startCopyName != null ) {
        JobEntryCopy startJobEntryCopy = jobMeta.findJobEntry( startCopyName );
        localJob.setStartJobEntryCopy( startJobEntryCopy );
      }

      localJob.start();

    } catch ( Throwable e ) {
      throw new ActionExecutionException( Messages.getInstance().getErrorString(
        "Kettle.ERROR_0022_JOB_START_FAILED" ), e );
    }

    // It's running in a separate thread to allow monitoring, etc.
    if ( log.isDebugEnabled() ) {
      log.debug( Messages.getInstance().getString( "Kettle.DEBUG_JOB_RUNNING" ) );
    }
    localJob.waitUntilFinished();
    int jobErrors = localJob.getErrors();
    long jobResultErrors = localJob.getResult().getNrErrors();
    if ( ( jobErrors > 0 ) || ( jobResultErrors > 0 ) ) {
      if ( log.isDebugEnabled() ) {
        log.debug( pdiUserAppender.getBuffer().toString() );
      }
      // don't throw exception, because the scheduler may try to run this job again
      log.error( org.pentaho.platform.plugin.kettle.messages.Messages.getInstance().getErrorString(
          "PdiAction.ERROR_0008_JOB_HAD_ERRORS",
          Integer.toString( jobErrors ), Long.toString( jobResultErrors ) ) );
      return;
    }

    // Dump the Kettle log...
    if ( log.isDebugEnabled() ) {
      log.debug( pdiUserAppender.getBuffer().toString() );
    }
  }

  /**
   * Connects to the PDI repository
   *
   * @return
   * @throws KettleException
   * @throws KettleSecurityException
   * @throws ActionExecutionException
   */
  protected Repository connectToRepository() throws KettleSecurityException, KettleException,
    ActionExecutionException {

    boolean singleDiServerInstance =
      "true".equals( PentahoSystem.getSystemSetting( SINGLE_DI_SERVER_INSTANCE, "true" ) );

    // Calling the kettle utility method to connect to the repository
    return RepositoryConnectionUtils.connectToRepository( repositoryName, singleDiServerInstance,
      PentahoSessionHolder.getSession().getName(), PentahoSystem.getApplicationContext().getFullyQualifiedServerURL(),
      pdiUserAppender );
  }

  @Override
  public void rowReadEvent( final RowMetaInterface row, final Object[] values ) {
  }

  /**
   * Processes a row of data generated by the PDI transform. This is a RowListener method
   */
  @Override
  public void rowWrittenEvent( final RowMetaInterface rowMeta, final Object[] row ) throws KettleStepException {
    processRow( transformationOutputRows, rowMeta, row );
  }

  /**
   * Processes an error row of data generated by the PDI transform. This is a RowListener method
   */
  @Override
  public void errorRowWrittenEvent( final RowMetaInterface rowMeta, final Object[] row ) throws KettleStepException {
    processRow( transformationOutputErrorRows, rowMeta, row );
  }

  /**
   * Adds a row of data to the provided result set
   *
   * @param memResults
   * @param rowMeta
   * @param row
   * @throws KettleStepException
   */
  public void processRow( MemoryResultSet memResults, final RowMetaInterface rowMeta, final Object[] row )
    throws KettleStepException {
    if ( memResults == null ) {
      return;
    }
    try {
      // create a new row object
      Object[] pentahoRow = new Object[memResults.getColumnCount()];
      for ( int columnNo = 0; columnNo < memResults.getColumnCount(); columnNo++ ) {
        // process each column in this row
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( columnNo );

        switch ( valueMeta.getType() ) {
          case ValueMetaInterface.TYPE_BIGNUMBER:
            pentahoRow[columnNo] = rowMeta.getBigNumber( row, columnNo );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            pentahoRow[columnNo] = rowMeta.getBoolean( row, columnNo );
            break;
          case ValueMetaInterface.TYPE_DATE:
            pentahoRow[columnNo] = rowMeta.getDate( row, columnNo );
            break;
          case ValueMetaInterface.TYPE_INTEGER:
            pentahoRow[columnNo] = rowMeta.getInteger( row, columnNo );
            break;
          case ValueMetaInterface.TYPE_NONE:
            pentahoRow[columnNo] = rowMeta.getString( row, columnNo );
            break;
          case ValueMetaInterface.TYPE_NUMBER:
            pentahoRow[columnNo] = rowMeta.getNumber( row, columnNo );
            break;
          case ValueMetaInterface.TYPE_STRING:
            pentahoRow[columnNo] = rowMeta.getString( row, columnNo );
            break;
          default:
            pentahoRow[columnNo] = rowMeta.getString( row, columnNo );
        }
      }
      // add the row to the result set
      memResults.addRow( pentahoRow );
    } catch ( KettleValueException e ) {
      throw new KettleStepException( e );
    }
  }

  /**
   * Sets the PDI repository (or filesystem) directory to load transformations and jobs from
   *
   * @param directory the directory from which to load transformations or jobs
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  /**
   * Sets any named inputs that need to be provided to the transformation or job
   */
  @Override
  public void setVarArgs( Map<String, Object> varArgs ) {
    this.varArgs = varArgs;
  }

  public String getLog() {
    return pdiUserAppender.getBuffer().toString();
  }

  /**
   * Returns the result set of the successful output rows. This will only return data if setMonitorStepName() or
   * setImportStepName() has been called
   *
   * @return
   */
  public MemoryResultSet getTransformationOutputRows() {
    return transformationOutputRows;
  }

  /**
   * Returns the result set of the error output rows. This will only return data if setMonitorStepName() or
   * setImportStepName() has been called
   *
   * @return
   */
  public MemoryResultSet getTransformationOutputErrorRows() {
    return transformationOutputErrorRows;
  }

  /**
   * Returns the number of successful output rows. This will only return data if setMonitorStepName() or
   * setImportStepName() has been called
   *
   * @return the number of successful output rows
   */
  public int getTransformationOutputRowsCount() {
    return transformationOutputRowsCount;
  }

  /**
   * Returns the number of failed output rows. This will only return data if setMonitorStepName() or setImportStepName()
   * has been called
   *
   * @return the number of failed output rows
   */
  public int getTransformationOutputErrorRowsCount() {
    return transformationOutputErrorRowsCount;
  }

  /**
   * Sets the result set containing rows to be injected into the transformation. This data will only be used if
   * setInjectorStep() is called.
   *
   * @param injectorRows
   */
  public void setInjectorRows( IPentahoResultSet injectorRows ) {
    this.injectorRows = injectorRows;
  }

  /**
   * Sets the name of the transformation to be loaded from the PDI repository. This is used in conjunction with
   * setDirectory().
   *
   * @param transformation the transformation name
   */
  public void setTransformation( String transformation ) {
    this.transformation = transformation;
  }

  /**
   * Sets the name of the job to be loaded from the PDI repository. This is used in conjunction with setDirectory().
   *
   * @param job the job name
   */
  public void setJob( String job ) {
    this.job = job;
  }

  /**
   * Sets the name of the transformation step to accept rows from
   *
   * @param monitorStep the step name from which to accept rows
   */
  public void setMonitorStep( String monitorStep ) {
    this.monitorStep = monitorStep;
  }

  /**
   * Returns the name of the transformation step to accept rows from
   *
   * @return the step name from which to accept rows
   */
  protected String getMonitorStepName() {
    return monitorStep;
  }

  /**
   * Sets the name of the transformation step to inject rows into. Use this in conjunction with setInjectorRows().
   *
   * @param injectorStep the step name to inject rows into
   */
  public void setInjectorStep( String injectorStep ) {
    this.injectorStep = injectorStep;
  }

  /**
   * Returns the status of the transformation or job
   *
   * @return the status of the transformation or job
   */
  public String getStatus() {
    if ( localTrans != null ) {
      return localTrans.getStatus();
    } else if ( localJob != null ) {
      return localJob.getStatus();
    } else {
      return Messages.getInstance().getErrorString( "Kettle.ERROR_0025_NOT_LOADED" );
    }
  }

  /**
   * Returns the exit status of the transformation or job
   *
   * @return the exit status of the transformation or job
   */
  public int getResult() {
    if ( localTrans != null ) {
      return localTrans.getResult().getExitStatus();
    } else if ( localJob != null ) {
      return localJob.getResult().getExitStatus();
    } else {
      return -1;
    }
  }

  public String getRepositoryName() {
    return repositoryName;
  }

  public void setRepositoryName( String repositoryName ) {
    this.repositoryName = repositoryName;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public void setLogLevel( String logLevel ) {
    this.logLevel = logLevel;
  }

  public String getClearLog() {
    return clearLog;
  }

  public void setClearLog( String clearLog ) {
    this.clearLog = clearLog;
  }

  public String getRunSafeMode() {
    return runSafeMode;
  }

  public void setRunSafeMode( String runSafeMode ) {
    this.runSafeMode = runSafeMode;
  }

  public String getRunClustered() {
    return runClustered;
  }

  public void setRunClustered( String runClustered ) {
    this.runClustered = runClustered;
  }

  public boolean isTransPrepareExecutionFailed() {
    return transPrepExecutionFailure;
  }

  public String getGatheringMetrics() {
    return gatheringMetrics;
  }

  public void setGatheringMetrics( String gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }

  public String getExpandingRemoteJob() {
    return expandingRemoteJob;
  }

  public void setExpandingRemoteJob( String expandingRemoteJob ) {
    this.expandingRemoteJob = expandingRemoteJob;
  }

  public String getStartCopyName() {
    return startCopyName;
  }

  public void setStartCopyName( String startCopyName ) {
    this.startCopyName = startCopyName;
  }

  public void setInputStream( InputStream inputStream ) {
    this.inputStream = inputStream;
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  /**
   * Checks to see if there is any error in preparation of KTR or if there are any errors
   * while executing the ktr/kjb
   * @return true if the execution was success, false otherwise
   */
  @Override
  public boolean isExecutionSuccessful() {
    boolean isSuccess;
    //Check if prevalidation failed
    isSuccess = !isTransPrepareExecutionFailed();
    if ( isSuccess ) {
      //Check if the transformation or jobs have step error
      if ( localTrans != null && localTrans.getErrors() > 0 ) {
        isSuccess = false;
      }
      //Check if the transformation or jobs have step error
      if ( localJob != null && localJob.getErrors() > 0 ) {
        isSuccess = false;
      }
    }
    return isSuccess;
  }
}
