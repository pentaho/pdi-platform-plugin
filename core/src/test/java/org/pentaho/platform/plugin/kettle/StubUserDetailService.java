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
