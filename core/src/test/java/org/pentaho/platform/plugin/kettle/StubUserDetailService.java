/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.platform.plugin.kettle;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.dao.DataAccessException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

public class StubUserDetailService implements UserDetailsService {

  @Override
  public UserDetails loadUserByUsername( String arg0 ) throws UsernameNotFoundException, DataAccessException {
    return new UserDetails() {

      @Override
      public boolean isEnabled() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean isCredentialsNonExpired() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean isAccountNonLocked() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean isAccountNonExpired() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public String getUsername() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String getPassword() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Collection<? extends GrantedAuthority> getAuthorities() {
        return Arrays.asList( new GrantedAuthority[] {} );
      }
    };
  }
}
