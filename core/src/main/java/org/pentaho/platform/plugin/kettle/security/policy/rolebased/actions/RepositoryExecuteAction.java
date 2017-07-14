package org.pentaho.platform.plugin.kettle.security.policy.rolebased.actions;

import java.util.ResourceBundle;

import org.pentaho.platform.plugin.kettle.messages.Messages;
import org.pentaho.platform.security.policy.rolebased.actions.AbstractAuthorizationAction;

public class RepositoryExecuteAction extends AbstractAuthorizationAction {
  public static final String NAME = "org.pentaho.repository.execute";
  ResourceBundle resourceBundle;
  
  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getLocalizedDisplayName(String localeString) {
    return Messages.getInstance().getString(NAME);
  }

}
