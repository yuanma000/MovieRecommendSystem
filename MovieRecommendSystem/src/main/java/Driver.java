public class Driver {
    public static void main(String[] args) throws Exception {

        RawDataSortedByUser raw = new RawDataSortedByUser();

        CoOccurrenceMatrix CoOccurrence = new CoOccurrenceMatrix();
        NormalizeCoOccurenceMatrix normalize = new NormalizeCoOccurenceMatrix();
        RatingMatrix rating = new RatingMatrix();//new
        MatrixMultiplication multiplication = new MatrixMultiplication();
        Sum sum = new Sum();

        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String normalizeDir = args[3];
        String ratingImprovement = args[6];
        String multiplicationDir = args[4];
        String sumDir = args[5];
        String[] path1 = {rawInput, userMovieListOutputDir};
        String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
        String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
//        String[] path4 = {normalizeDir, rawInput, multiplicationDir};
        String[] path6 = {rawInput, ratingImprovement};//new
        String[] path4 = {normalizeDir, ratingImprovement, multiplicationDir};
        String[] path5 = {multiplicationDir, sumDir};


        raw.main(path1);
        CoOccurrence.main(path2);
        normalize.main(path3);
        rating.main(path6);//new
        multiplication.main(path4);
        sum.main(path5);

    }
}
