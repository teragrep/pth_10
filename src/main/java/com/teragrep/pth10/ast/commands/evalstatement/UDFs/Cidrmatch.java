/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2025 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth10.ast.commands.evalstatement.UDFs;

import com.teragrep.pth10.ast.TextString;
import com.teragrep.pth10.ast.UnquotedText;
import org.apache.spark.sql.api.java.UDF2;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * User Defined Function for command cidrmatch(ip, subnet)<br>
 * <br>
 * Assumes that both ip and subnet are in String format, and returns a boolean.<br>
 * Returns TRUE, if the ip belongs to the subnet given. Otherwise returns FALSE.<br>
 * Expects subnet to be in CIDR form. Like 192.168.1.1/24. Where the number after / is the netmask length in bits.<br>
 * <br>
 * Netmask length explanation:<br>
 * Until 255.x.x.x netmask=8<br>
 * Until 255.255.x.x netmask=16<br>
 * Until 255.255.255.x netmask=24<br>
 * Until 255.255.255.255 netmask=32<br>
 * 
 * @author eemhu
 */
public class Cidrmatch implements UDF2<String, String, Boolean>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public Boolean call(String ip, String subnet) throws Exception {

        // Strip quotes, if any
        subnet = new UnquotedText(new TextString(subnet)).read();
        ip = new UnquotedText(new TextString(ip)).read();

        int nMaskBits;
        InetAddress requiredAdd = null;

        // Check for subnet mask length in bits (given after '/' character)
        if (subnet.indexOf('/') > 0) {
            String[] addWithMask = subnet.split("/");

            subnet = addWithMask[0];
            nMaskBits = Integer.parseInt(addWithMask[1]);
        }
        else {
            // Set to -1 if subnet mask length was not given
            nMaskBits = -1;
        }

        // Convert subnet string to InetAddress object
        try {
            requiredAdd = InetAddress.getByName(subnet);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(
                    "Cidrmatch could not convert subnet string to InetAddress object. Check that the string is a valid IP address, like 192.168.1.1."
            );
        }

        // Convert ip string to InetAddress object
        InetAddress remoteAdd = null;
        try {
            remoteAdd = InetAddress.getByName(ip);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(
                    "Cidrmatch could not convert IP string to InetAddress object. Check that the string is a valid IP address, like 192.168.1.1."
            );
        }

        // Check that both are valid InetAddress objects
        if (!requiredAdd.getClass().equals(remoteAdd.getClass())) {
            return false;
        }

        // If no subnet mask was found, do a direct comparison.
        if (nMaskBits < 0) {
            boolean isSame = remoteAdd == requiredAdd;

            return isSame;
        }

        // Subnet mask was given, check against given mask
        byte[] remAddr = remoteAdd.getAddress();
        byte[] reqAddr = requiredAdd.getAddress();

        int nMaskFullBytes = nMaskBits / 8;

        byte finalByte = (byte) (0xFF00 >> (nMaskBits & 0x07));

        for (int i = 0; i < nMaskFullBytes; i++) {
            if (remAddr[i] != reqAddr[i]) {
                return false;
            }
        }

        if (finalByte != 0) {
            boolean isSame = (remAddr[nMaskFullBytes] & finalByte) == (reqAddr[nMaskFullBytes] & finalByte);

            return isSame;
        }

        return true;
    }

}
