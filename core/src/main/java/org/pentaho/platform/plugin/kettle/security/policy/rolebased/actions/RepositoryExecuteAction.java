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

package org.pentaho.platform.plugin.kettle.security.policy.rolebased.actions;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.pentaho.platform.plugin.kettle.messages.Messages;
import org.pentaho.platform.security.policy.rolebased.actions.AbstractLocalizedAuthorizationAction;

public class RepositoryExecuteAction extends AbstractLocalizedAuthorizationAction {
  public static final String NAME = "org.pentaho.repository.execute";

  @NonNull
  @Override
  public String getName() {
    return NAME;
  }

  @NonNull
  @Override
  public String getLocalizedDisplayName( @Nullable String localeString ) {
    return Messages.getInstance().getString( NAME );
  }

  @Nullable
  @Override
  public String getLocalizedDescription( @Nullable String localeString ) {
    return Messages.getInstance().getString( NAME + ".description" );
  }
}
