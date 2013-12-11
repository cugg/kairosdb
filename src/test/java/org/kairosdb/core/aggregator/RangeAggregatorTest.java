//
// RangeAggregatorTest.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.aggregator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Ignore;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.Sampling;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RangeAggregatorTest
{
	@Test
	public void test_yearRange()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 0; I < 12; I++)
		{
			cal.clear();
			cal.set(2012, I, 1, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(1, TimeUnit.YEARS));
		agg.setAlignSampling(true);
		cal.clear();
		cal.set(2012, 0, 0, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(12L));

		assertThat(dpg.hasNext(), is(false));
	}


	@Test
	public void test_monthRange()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 32; I++)
		{
			cal.clear();
			cal.set(2012, 0, I, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(1, TimeUnit.MONTHS));
		agg.setAlignSampling(true);
		cal.clear();
		cal.set(2012, 0, 1, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		/*while (dpg.hasNext())
			System.out.println(dpg.next().getLongValue());*/

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(31L));

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(1L));

		assertThat(dpg.hasNext(), is(false));
	}

	@Test
	public void test_mulitpleMonths()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 70; I++)
		{
			cal.clear();
			cal.set(2012, 0, I, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(2, TimeUnit.MONTHS,TimeZone.getTimeZone("Europe/Paris")));
		agg.setAlignSampling(true);
		cal.clear();
		cal.set(2012, 0, 1, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		/*while (dpg.hasNext())
			System.out.println(dpg.next().getLongValue());*/

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(60L));

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(10L));

		assertThat(dpg.hasNext(), is(false));
	}

	@Test
	public void test_midMonthStart()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 32; I++)
		{
			cal.clear();
			cal.set(2012, 0, I+15, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(1, TimeUnit.MONTHS));
		cal.clear();
		cal.set(2012, 0, 16, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		DataPointGroup dpg = agg.aggregate(dpGroup);

		/*while (dpg.hasNext())
			System.out.println(dpg.next().getLongValue());*/

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(31L));

		assertThat(dpg.hasNext(), is(true));
		assertThat(dpg.next().getLongValue(), is(1L));

		assertThat(dpg.hasNext(), is(false));
	}

	@Test
	public void test_alignOnWeek()
	{
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

		for (int I = 1; I <= 32; I++)
		{
			cal.clear();
			cal.set(2012, 0, I+15, 1, 1, 1);
			dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
		}

		SumAggregator agg = new SumAggregator();
		agg.setSampling(new Sampling(1, TimeUnit.WEEKS));
		agg.setAlignSampling(true);
		cal.clear();
		cal.set(2012, 0, 16, 0, 0, 0);
		agg.setStartTime(cal.getTimeInMillis());

		//Just making sure the alignment doesn't blow up
		DataPointGroup dpg = agg.aggregate(dpGroup);
	}

    @Test
    // not working with old method
    public void test_multipleYears()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 0; I < 24; I++)
        {
            cal.clear();
            cal.set(2011, I, 1, 1, 1, 1);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(2, TimeUnit.YEARS));
        agg.setAlignSampling(true);
        cal.clear();
        cal.set(2011, 0, 0, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(24L));

        assertThat(dpg.hasNext(), is(false));
    }


    @Test
    public void test_monthRangeWithHourlyDataPoints()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 0; I < 768; I++)
        {
            cal.clear();
            cal.set(2012, 0, 1, I, 0, 0);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(1, TimeUnit.MONTHS));
        cal.clear();
        cal.set(2012, 0, 1, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(744L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(24L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test //not working with old method
    public void test_multipleMonthsFebStart()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 70; I++)
        {
            cal.clear();
            cal.set(2012, 1, I, 1, 1, 1);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(2, TimeUnit.MONTHS, TimeZone.getTimeZone("Europe/Paris")));
        agg.setAlignSampling(true);
        cal.clear();
        cal.set(2012, 1, 1, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(60L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(10L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    public void test_tripleMonths()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 100; I++)
        {
            cal.clear();
            cal.set(2012, 0, I, 1, 1, 1);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(3, TimeUnit.MONTHS, TimeZone.getTimeZone("Europe/Paris")));
//        agg.setAlignSampling(true);

        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(91L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(9L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
//  not working with old method
    public void test_tripleMonthsFebStart()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 1; I <= 100; I++)
        {
            cal.clear();
            cal.set(2012, 1, I, 1, 1, 1);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(3, TimeUnit.MONTHS,TimeZone.getTimeZone("Europe/Paris")));
        agg.setAlignSampling(true);
        cal.clear();
        cal.set(2012, 1, 1, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(90L));

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(10L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    public void test_multipleDays()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 0; I < 216; I++)
        {
            cal.clear();
            cal.set(2012, 0, 1, I, 0, 0);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(2, TimeUnit.DAYS));
//        agg.setAlignSampling(true);

        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(48L));
        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(48L));
        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(48L));
        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(48L));
        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(24L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    //not working with old method
    public void test_tripleDays()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 0; I < 216; I++)
        {
            cal.clear();
            cal.set(2012, 0, 1, I, 0, 0);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(3, TimeUnit.DAYS));


        DataPointGroup dpg = agg.aggregate(dpGroup);

        /*while (dpg.hasNext())
              System.out.println(dpg.next().getLongValue());*/

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(72L));
        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(72L));
        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(72L));

        assertThat(dpg.hasNext(), is(false));
    }

    @Test
    //not working with old method
    public void testSummerDST() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
        Long valueOn25March = 0L;
        Long valueOn24March = 0L;
        Long valueOn26March = 0L;
        Long valueOn27March = 0L;
        DateTime dateTime;
        for (int d = 19; d < 30; d++) {
            for (int h = 0; h < 24; h++) {

                cal.set(2012, 2, d, h, 0, 0);
                Long val=new Long((h+1)*d);
                dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), val));
                DateTime dt= new DateTime(cal.getTimeInMillis());
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("24"))
                    valueOn24March+=val;
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("25"))
                    valueOn25March+=val;
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("26"))
                    valueOn26March+=val;
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("27"))
                    valueOn27March+=val;
            }
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(1, TimeUnit.DAYS, TimeZone.getTimeZone("Europe/Paris")));
        DataPointGroup dpg = agg.aggregate(dpGroup);
        while (dpg.hasNext()) {

            DataPoint dp = dpg.next();
            dateTime=new DateTime(dp.getTimestamp());

            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("24")) assertThat(dp.getLongValue(), is(valueOn24March));
            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("25")) assertThat(dp.getLongValue(), is(valueOn25March));
            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("26")) assertThat(dp.getLongValue(), is(valueOn26March));
            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("27")) assertThat(dp.getLongValue(), is(valueOn27March));

        }

    }

    @Test
    //not working with old method
    public void testWinterDST() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");
        Long valueOn27October = 0L;
        Long valueOn28October = 0L;
        Long valueOn29October = 0L;
        Long valueOn30October = 0L;
        DateTime dateTime;
        for (int d = 27; d <= 30; d++) {
            for (int h = 0; h < 24; h++) {

                cal.set(2012, 9, d, h, 0, 0);
                Long val=new Long((h+1)*d);
                dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), val));
                DateTime dt= new DateTime(cal.getTimeInMillis());
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("27"))
                    valueOn27October+=val;
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("28"))
                    valueOn28October+=val;
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("29"))
                    valueOn29October+=val;
                if (dt.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("30"))
                    valueOn30October+=val;
            }
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(1, TimeUnit.DAYS, TimeZone.getTimeZone("Europe/Paris")));
        DataPointGroup dpg = agg.aggregate(dpGroup);
        while (dpg.hasNext()) {

            DataPoint dp = dpg.next();
            dateTime=new DateTime(dp.getTimestamp());

            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("24")) assertThat(dp.getLongValue(), is(valueOn27October));
            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("28")) assertThat(dp.getLongValue(), is(valueOn28October));
            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("29")) assertThat(dp.getLongValue(), is(valueOn29October));
            if (dateTime.withZone(DateTimeZone.forID("Europe/Paris")).toString("dd").equals("30")) assertThat(dp.getLongValue(), is(valueOn30October));

        }

    }

    @Test
    @Ignore //not working with both methods
    public void test_midYearStart()
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        ListDataPointGroup dpGroup = new ListDataPointGroup("range_test");

        for (int I = 0; I < 12; I++)
        {
            cal.clear();
            System.out.println(2013-I/6);
            cal.set(2013-I/6, I, 1, 1, 1, 1);
            dpGroup.addDataPoint(new DataPoint(cal.getTimeInMillis(), 1));
        }

        SumAggregator agg = new SumAggregator();
        agg.setSampling(new Sampling(1, TimeUnit.YEARS, TimeZone.getTimeZone("Europe/Paris")));
        cal.clear();
        cal.set(2012, 5, 1, 0, 0, 0);
        agg.setStartTime(cal.getTimeInMillis());

        DataPointGroup dpg = agg.aggregate(dpGroup);

        assertThat(dpg.hasNext(), is(true));
        assertThat(dpg.next().getLongValue(), is(12L));

        assertThat(dpg.hasNext(), is(false));
    }
}
