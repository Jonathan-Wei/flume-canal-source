package com.citic.source.canal;


import junit.framework.Assert;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.List;

public class TestCanalConf {

    @Test(expected = IllegalArgumentException.class)
    public void testConvertUrlsToSocketAddressListMalformed() throws IllegalArgumentException {
        CanalConf.convertUrlsToSocketAddressList("192.168.100.101:11111,192.168.100.101");
    }

    @Test
    public void testConvertUrlsToSocketAddressList() throws IllegalArgumentException {
        List<SocketAddress> list = CanalConf.convertUrlsToSocketAddressList(
            "192.168.100.101:11111,192.168.100.102:10000,192.168.100.103:11111");

        Assert.assertEquals(list.size(), 3);
    }
}
