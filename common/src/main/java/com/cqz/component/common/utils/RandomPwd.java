package com.cqz.component.common.utils;

import java.util.Random;
import java.util.Scanner;

public class RandomPwd {

    public static void main(String[] args) {
        String password = randomPassword(getInput());
        System.out.println("您随机出来的密码为:"+password.toString());
    }

    /** 获取用户输入的密码位数 */
    public static int getInput(){
        System.out.println("请输入您想获取的密码位数:");
        return new Scanner(System.in).nextInt();
    }

    /** 随机出用户输入的密码位数的密码,从大小写字母,数字中取值 */
    public static String randomPassword(int num){
        char[] passwor = new char[num];//创建char数组接收每一位随机出来的密码
        Random rand = new Random();
        //在ASCII码表中,48-57 数字,65-90 大写字母,97-122 小写字母
        for (int i = 0; i <passwor.length ; i++) {
            int choice = rand.nextInt(3);
            int lowercase = rand.nextInt(26)+65;//小写字母ASCII码表范围
            int uppercase = rand.nextInt(26)+97;//大写字母ASCII码表范围
            int figure = rand.nextInt(10)+48;//数字ASCII码表范围
            switch (choice){//从大写字母.小写字母.数字中随机取值
                case 0:passwor[i]=(char)lowercase;break;
                case 1:passwor[i]=(char)uppercase;break;
                case 2:passwor[i]=(char)figure;
            }
        }
        return new String(passwor);
    }

}
