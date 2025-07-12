#!/usr/bin/env python3
"""
Test script to verify Bybit API integration
Run this to test the API endpoints before deploying
"""

import asyncio
import requests
from datetime import datetime
import sys

BYBIT_API_BASE = "https://api.bybit.com/v5"

def test_spot_price():
    """Test fetching BTC spot price"""
    print("🔍 Testing BTC spot price fetch...")
    
    try:
        url = f"{BYBIT_API_BASE}/market/tickers"
        params = {"category": "linear", "symbol": "BTCUSDT"}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            print(f"❌ API Error: {data.get('retMsg')}")
            return False
            
        result = data.get("result", {}).get("list", [])
        if not result:
            print("❌ No spot price data returned")
            return False
            
        spot_price = float(result[0].get("lastPrice"))
        print(f"✅ BTC Spot Price: ${spot_price:,.2f}")
        return True
        
    except Exception as e:
        print(f"❌ Error fetching spot price: {e}")
        return False

def test_options_instruments():
    """Test fetching BTC options instruments"""
    print("\n🔍 Testing BTC options instruments fetch...")
    
    try:
        url = f"{BYBIT_API_BASE}/market/instruments-info"
        params = {"category": "option", "baseCoin": "BTC", "limit": 10}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            print(f"❌ API Error: {data.get('retMsg')}")
            return False
            
        instruments = data.get("result", {}).get("list", [])
        if not instruments:
            print("❌ No options instruments returned")
            return False
            
        print(f"✅ Found {len(instruments)} BTC options instruments")
        
        # Show a few examples
        print("📋 Sample instruments:")
        for i, instrument in enumerate(instruments[:3]):
            symbol = instrument.get('symbol', 'N/A')
            status = instrument.get('status', 'N/A')
            option_type = instrument.get('optionsType', 'N/A')
            delivery_time = instrument.get('deliveryTime', 'N/A')
            
            if delivery_time != 'N/A':
                expiry_dt = datetime.fromtimestamp(int(delivery_time) / 1000)
                expiry_str = expiry_dt.strftime('%Y-%m-%d')
            else:
                expiry_str = 'N/A'
                
            print(f"  {i+1}. {symbol} | {option_type} | {status} | Expires: {expiry_str}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error fetching options instruments: {e}")
        return False

def test_option_ticker():
    """Test fetching option ticker data"""
    print("\n🔍 Testing option ticker fetch...")
    
    # First get an instrument to test with
    try:
        url = f"{BYBIT_API_BASE}/market/instruments-info"
        params = {"category": "option", "baseCoin": "BTC", "limit": 50}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            print(f"❌ API Error: {data.get('retMsg')}")
            return False
            
        instruments = data.get("result", {}).get("list", [])
        
        # Find a trading instrument
        test_symbol = None
        for instrument in instruments:
            if instrument.get('status') == 'Trading':
                test_symbol = instrument.get('symbol')
                break
                
        if not test_symbol:
            print("❌ No trading instruments found")
            return False
            
        print(f"📊 Testing ticker for: {test_symbol}")
        
        # Now test ticker fetch
        url = f"{BYBIT_API_BASE}/market/tickers"
        params = {"category": "option", "symbol": test_symbol}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            print(f"❌ API Error: {data.get('retMsg')}")
            return False
            
        result = data.get("result", {}).get("list", [])
        if not result:
            print("❌ No ticker data returned")
            return False
            
        ticker = result[0]
        mark_price = ticker.get('markPrice', 'N/A')
        iv = ticker.get('markIv', 'N/A')
        bid = ticker.get('bid1Price', 'N/A')
        ask = ticker.get('ask1Price', 'N/A')
        
        print(f"✅ Ticker Data:")
        print(f"   Mark Price: ${mark_price}")
        print(f"   Implied Vol: {iv}")
        print(f"   Bid: ${bid}")
        print(f"   Ask: ${ask}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fetching option ticker: {e}")
        return False

def test_moneyness_logic():
    """Test the moneyness calculation logic"""
    print("\n🔍 Testing moneyness calculation logic...")
    
    try:
        # Get spot price
        url = f"{BYBIT_API_BASE}/market/tickers"
        params = {"category": "linear", "symbol": "BTCUSDT"}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get("retCode") != 0:
            print(f"❌ API Error: {data.get('retMsg')}")
            return False
            
        result = data.get("result", {}).get("list", [])
        spot_price = float(result[0].get("lastPrice"))
        
        # Calculate target strikes
        target_strikes = {
            'ITM': spot_price * 0.95,  # 5% ITM
            'ATM': spot_price,         # ATM
            'OTM': spot_price * 1.05   # 5% OTM
        }
        
        print(f"✅ Moneyness Calculation (Spot: ${spot_price:,.2f}):")
        for option_type, target_strike in target_strikes.items():
            print(f"   {option_type}: Target strike ${target_strike:,.2f}")
            
        # Get options and find closest matches
        url = f"{BYBIT_API_BASE}/market/instruments-info"
        params = {"category": "option", "baseCoin": "BTC", "limit": 1000}
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        instruments = data.get("result", {}).get("list", [])
        
        # Find target expiry (closest to 7 days)
        from datetime import timedelta
        now = datetime.utcnow()
        target_time = now + timedelta(days=7)
        
        best_expiry = None
        best_diff = float('inf')
        
        for option in instruments:
            if option.get('status') != 'Trading':
                continue
                
            delivery_time = option.get('deliveryTime')
            if not delivery_time:
                continue
                
            expiry_dt = datetime.fromtimestamp(int(delivery_time) / 1000)
            diff = abs((expiry_dt - target_time).total_seconds())
            
            if diff < best_diff:
                best_diff = diff
                best_expiry = delivery_time
        
        if not best_expiry:
            print("❌ No suitable expiry found")
            return False
            
        expiry_dt = datetime.fromtimestamp(int(best_expiry) / 1000)
        print(f"✅ Target expiry: {expiry_dt.strftime('%Y-%m-%d %H:%M')} UTC")
        
        # Find moneyness options
        filtered_options = [
            opt for opt in instruments 
            if (opt.get('deliveryTime') == best_expiry and 
                opt.get('optionsType') == 'Call' and
                opt.get('status') == 'Trading')
        ]
        
        print(f"✅ Found {len(filtered_options)} call options for target expiry")
        
        result = {'ITM': None, 'ATM': None, 'OTM': None}
        
        for option_type, target_strike in target_strikes.items():
            best_option = None
            best_diff = float('inf')
            
            for option in filtered_options:
                symbol_parts = option['symbol'].split('-')
                if len(symbol_parts) < 4:
                    continue
                    
                try:
                    strike = float(symbol_parts[2])
                    diff = abs(strike - target_strike)
                    
                    if diff < best_diff:
                        best_diff = diff
                        best_option = option['symbol']
                except (ValueError, IndexError):
                    continue
            
            result[option_type] = best_option
            if best_option:
                strike = float(best_option.split('-')[2])
                print(f"   {option_type}: {best_option} (Strike: ${strike:,.0f})")
            else:
                print(f"   {option_type}: No suitable option found")
        
        return all(result.values())
        
    except Exception as e:
        print(f"❌ Error in moneyness logic: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 BTC Options Tracker - Bybit API Test")
    print("=" * 50)
    
    tests = [
        ("Spot Price", test_spot_price),
        ("Options Instruments", test_options_instruments),
        ("Option Ticker", test_option_ticker),
        ("Moneyness Logic", test_moneyness_logic),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running {test_name} test...")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
        if success:
            passed += 1
    
    print(f"\n🎯 Results: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Bybit API integration is working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())