package org.myPaper.additionalClasses;

/**
 * Normalizes a number in the range of 0 and 1.
 */
public class NormalizeZeroOne {
    /**
     * Normalizes a number in the range of 0 and 1.
     *
     * @param nonNormalizeNumber the non normalized number
     * @param max                the maximum value in that range
     * @param min                the minimum value in that range
     * @return the normalized number in the range of 0 and 1
     */
    public static double normalize(double nonNormalizeNumber, double max, double min) {
        if (max - min == 0) {
            throw new IllegalStateException("The sum of max and min could not be zero!");
        }

        return (nonNormalizeNumber - min) / (max - min);
    }
}
