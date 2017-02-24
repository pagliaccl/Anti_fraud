package com.iflytek.vine.tools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Linxiao Bai
 */
public class PearsonCorrelationScore implements Serializable{

    public Map<String, Map<String, Double>> dataset = null;



    public static double sim_pearson(Map<String, Double> person1, Map<String, Double> person2) {
        List<String> list = new ArrayList<String>();
        for (Entry<String, Double> p1 : person1.entrySet()) {
            if (person2.containsKey(p1.getKey())) {
                list.add(p1.getKey());
            }
        }

        double sumX = 0.0;
        double sumY = 0.0;
        double sumX_Sq = 0.0;
        double sumY_Sq = 0.0;
        double sumXY = 0.0;
        int N = list.size();

        for (String name : list) {
            Map<String, Double> p1Map = person1;
            Map<String, Double> p2Map = person2;

            sumX += p1Map.get(name);
            sumY += p2Map.get(name);
            sumX_Sq += Math.pow(p1Map.get(name), 2);
            sumY_Sq += Math.pow(p2Map.get(name), 2);
            sumXY += p1Map.get(name) * p2Map.get(name);
        }

        double numerator = sumXY - sumX * sumY / N;
        double denominator = Math.sqrt((sumX_Sq - sumX * sumX / N)
                * (sumY_Sq - sumY * sumY / N));

        // Denominator Not 0
        if (denominator == 0) {
            return 0;
        }

        return numerator / denominator;
    }

    /*Test method*/
    public static void main(String[] args) {
        PearsonCorrelationScore pearsonCorrelationScore = new PearsonCorrelationScore();
        Map<String,Double> p1= pearsonCorrelationScore.dataset.get("Jack Matthews");
        Map<String,Double> p2= pearsonCorrelationScore.dataset.get("Lisa Rose");

        System.out.println(pearsonCorrelationScore.sim_pearson(p1, p2));
    }

}
