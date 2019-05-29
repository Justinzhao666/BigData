package project.weibo;

import project.weibo.constant.Constant;
import project.weibo.util.WeiBoUtil;

import java.io.IOException;

public class WeiBo {

    public static void init() throws IOException {
        //创建Namespace
        WeiBoUtil.createNamespace(Constant.NAMESPACE);
        //建表
        WeiBoUtil.createTable(Constant.CONTENT,1,"info");
        WeiBoUtil.createTable(Constant.INBOX,2,"info");
        WeiBoUtil.createTable(Constant.RELATIONS,1,"attends","fans");
    }

    public static void main(String[] args) throws IOException {
        init();
        //1001.1002发微博
        WeiBoUtil.createData("1001","i am 1001");
        WeiBoUtil.createData("1002","i am 1002");
        //1001 关注1003.1002
        WeiBoUtil.addAttend("1001","1003","1002");
        //1001 看朋友圈
        WeiBoUtil.getFriends("1001");
        //1003 发个朋友圈
        WeiBoUtil.createData("1003","i am 1003");
        //1001 再看下朋友圈
        WeiBoUtil.getFriends("1001");
    }
}
