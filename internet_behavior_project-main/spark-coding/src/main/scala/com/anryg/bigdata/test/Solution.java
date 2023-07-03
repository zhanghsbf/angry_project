package com.anryg.bigdata.test;

class Solution {
    public static void main(String[] args) {
        int[] arr = {-1,0,3,5,9,12};
        System.out.println(search(arr, 2));
    }
    public static int search(int[] nums, int target) {
        int k = nums.length / 2;
        while(k != 0 || k != (nums.length -1)){
            if(nums[k] > target){
                k = k / 2;
            } else if (nums[k] < target){
                k = k + (nums.length - k) / 2;
            } else {
                return k;
            }
        }

        return -1;
    }
}
