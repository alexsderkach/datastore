package io.datastore.worker.support;

import lombok.NoArgsConstructor;
import lombok.extern.java.Log;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.logging.Level;

import static lombok.AccessLevel.PRIVATE;

@Log
@NoArgsConstructor(access = PRIVATE)
public class NetworkUtil {

  public static Optional<String> getId() {
    try {
      InetAddress hostAddress = InetAddress.getLocalHost();
      byte[] mac = NetworkInterface.getByInetAddress(hostAddress).getHardwareAddress();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < mac.length; i++) {
        sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
      }
      return Optional.of(sb.toString());
    } catch (UnknownHostException | SocketException e) {
      log.severe(() -> "Couldn't fetch mac address: " + e.getMessage());
    }
    return Optional.empty();
  }
}
