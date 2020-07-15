package com.test.yaowentao;

import java.util.*;
import java.io.*;

public class Demo1_1 {
    public static void main(String[] args) throws Exception {
        ManagerEmp m1 = new ManagerEmp();

        BufferedReader buff=new BufferedReader(new InputStreamReader(System.in));

        while (true){
            System.out.println("请选择你要进行的操作：");
            System.out.println("1:新增一个雇员");
            System.out.println("2:删除一个雇员");
            System.out.println("3:查询雇员信息");
            System.out.println("4:修改雇员薪水");
            System.out.println("5:退出系统");

            String orderType= buff.readLine();
            if(orderType.equals("1")){
                //获取员工编号
                System.out.println("请输入雇员编号：");
                String empNo = buff.readLine();

                //获取雇员的名称
                System.out.println("请输入雇员名称：");
                String name = buff.readLine();

                //获取雇员的薪水
                System.out.println("请输入雇员薪水：");
                float sal = Float.parseFloat(buff.readLine());

                Emp emp=new Emp(empNo,name,sal);
                m1.addEmp(emp);
            }
            else if(orderType.equals("2")){
                //获取员工编号
                System.out.println("请输入雇员编号：");
                String empNo = buff.readLine();

                m1.delEmp(empNo);
            }
            else if(orderType.equals("3")){
                //获取员工编号
                System.out.println("请输入雇员编号：");
                String empNo = buff.readLine();

                m1.showInfo(empNo);
            }
            else if(orderType.equals("4")){
                //获取员工编号
                System.out.println("请输入雇员编号：");
                String empNo = buff.readLine();

                //获取员工编号
                System.out.println("请输入雇员薪水：");
                float sal = Float.parseFloat(buff.readLine());

                m1.modSal(empNo,sal);
            }
            else if(orderType.equals("5")){
                m1.exit();
            }
        }
    }
}

class ManagerEmp{
    private  ArrayList al=null;

    public ManagerEmp(){
        al= new ArrayList();
    }

    //新增一个雇员
    public void addEmp(Emp emp) {
        al.add(emp);
        //System.out.println((Emp)al.get(0));
    }

    //删除一个雇员
    public void delEmp(String empNo){
        for(int i=0;i<al.size();i++){
            Emp emp=(Emp)al.get(i);
            if(emp.getEmpNo().equals(empNo)){
                al.remove(i);
            }
        }
    }

    //查询雇员信息
    public void showInfo(String empNo){
        for(int i=0; i<al.size();i++){
            Emp emp=(Emp)al.get(i);
            //取出Emp对象
            if(emp.getEmpNo().equals(empNo)){
                System.out.println("雇员信息是： ");
                System.out.println("雇员编号： " + emp.getEmpNo());
                System.out.println("雇员名称： " + emp.getName());
                System.out.println("雇员薪资： " + emp.getSal());
            }
        }
    }

    //修改雇员的薪水
    public void modSal(String empNo,Float newSal){
        for(int i=0; i<al.size();i++){
            Emp emp=(Emp)al.get(i);

            if(emp.getEmpNo().equals(empNo)){
                emp.setSal(newSal);
            }
        }
    }

    //退出系统
    public void exit(){
        System.exit(0);
    }
}

class Emp{
    private String empNo;
    private String name;
    private Float sal;

    public Emp(String empNo,String name,Float sal){
        this.empNo=empNo;
        this.name=name;
        this.sal=sal;
    }

    @Override
    public String toString() {
        return "Emp{" +
                "empNo='" + empNo + '\'' +
                ", name='" + name + '\'' +
                ", sal=" + sal +
                '}';
    }

    public String getEmpNo() {
        return empNo;
    }

    public void setEmpNo(String empNo) {
        this.empNo = empNo;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getSal() {
        return sal;
    }

    public void setSal(Float sal) {
        this.sal = sal;
    }
}