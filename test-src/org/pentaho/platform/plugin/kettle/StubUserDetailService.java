package org.pentaho.platform.plugin.kettle;

import java.util.Arrays;

import org.springframework.dao.DataAccessException;
import org.springframework.security.GrantedAuthority;
import org.springframework.security.userdetails.UserDetails;
import org.springframework.security.userdetails.UserDetailsService;
import org.springframework.security.userdetails.UsernameNotFoundException;

public class StubUserDetailService implements UserDetailsService {

  @Override
  public UserDetails loadUserByUsername(String arg0) throws UsernameNotFoundException, DataAccessException {
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
      public GrantedAuthority[] getAuthorities() {
        return new GrantedAuthority[]{};
      }
    };
  }
}
