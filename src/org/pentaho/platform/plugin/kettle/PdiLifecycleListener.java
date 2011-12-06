package org.pentaho.platform.plugin.kettle;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.platform.api.engine.IPluginLifecycleListener;
import org.pentaho.platform.api.engine.PluginLifecycleException;

public class PdiLifecycleListener implements IPluginLifecycleListener
{

  public void init() throws PluginLifecycleException
  {
    try {
      KettleSystemListener.environmentInit(null);
    } catch (KettleException e) {
      throw new PluginLifecycleException(e);
    }
  }

  public void loaded() throws PluginLifecycleException
  {
  }

  public void unLoaded() throws PluginLifecycleException
  {
  }

}
