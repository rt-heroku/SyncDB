package com.heroku.syncdbs;

import java.util.Arrays;

public class TestMaterializeViews {
    public static final String VIEWS = "View_Seal_Account_List;View_Seal_CE_List;View_Seal_Campaign_List;View_Seal_Case_List;View_Seal_Contact_List;View_Seal_GroupID_List;View_Seal_Opportunity_List";
    
    public static void main(String[] args) {
        String[] data = VIEWS.split("\\;");
        System.out.println(Arrays.toString(data));
    }

}
