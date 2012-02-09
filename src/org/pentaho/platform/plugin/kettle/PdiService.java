/*
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
 * Copyright 2006 - 2012 Pentaho Corporation.  All rights reserved.
 *
 */
package org.pentaho.platform.plugin.kettle;

import java.util.HashMap;
import java.util.Map;

public class PdiService {

  protected String itemName;

  protected boolean usePdiRepo = false;

  public PdiServiceResult executeTransformation(String filename, String parameter1, String parameter2, String parameter3, String parameter4, String parameter5,
      String parameter6, String parameter7, String parameter8, String parameter9, String parameter10) {

    // create the PDI component
    PdiAction pdiComponent = new PdiAction();

    // see if we are running a transformation or job
    if (filename.toLowerCase().endsWith(".ktr")) { //$NON-NLS-1$
      pdiComponent.setTransformation(filename);
    } else if (filename.toLowerCase().endsWith(".kjb")) { //$NON-NLS-1$
      pdiComponent.setJob(filename);
    }

    // create a map of the inputs
    Map<String, Object> inputs = new HashMap<String, Object>();
    if (parameter1 != null) {
      inputs.put("parameter1", parameter1); //$NON-NLS-1$
    }
    if (parameter2 != null) {
      inputs.put("parameter2", parameter2); //$NON-NLS-1$
    }
    if (parameter3 != null) {
      inputs.put("parameter3", parameter3); //$NON-NLS-1$
    }
    if (parameter4 != null) {
      inputs.put("parameter4", parameter4); //$NON-NLS-1$
    }
    if (parameter5 != null) {
      inputs.put("parameter5", parameter5); //$NON-NLS-1$
    }
    if (parameter6 != null) {
      inputs.put("parameter6", parameter6); //$NON-NLS-1$
    }
    if (parameter7 != null) {
      inputs.put("parameter7", parameter7); //$NON-NLS-1$
    }
    if (parameter8 != null) {
      inputs.put("parameter8", parameter8); //$NON-NLS-1$
    }
    if (parameter9 != null) {
      inputs.put("parameter9", parameter9); //$NON-NLS-1$
    }
    if (parameter10 != null) {
      inputs.put("parameter10", parameter10); //$NON-NLS-1$
    }
    pdiComponent.setVarArgs(inputs);

    // now execute
    try {
      pdiComponent.execute();
    } catch (Exception ex) {
      // ok used to be set to false, but was never used!?
    }

    PdiServiceResult result = new PdiServiceResult();
    result.setStatus(pdiComponent.getStatus());
    result.setResult(pdiComponent.getResult());
    result.setLog(pdiComponent.getLog());
    return result;

  }

}
