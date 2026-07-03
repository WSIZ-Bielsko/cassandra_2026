import platform
print(platform.processor())
print(platform.machine())


import cpuinfo
print(cpuinfo.get_cpu_info()['brand_raw'])


import psutil

cpu_freq = psutil.cpu_freq(percpu=True)
for cpu in cpu_freq:
    print(f"Current Frequency: {cpu.current} MHz")
    print(f"Minimum Frequency: {cpu.min} MHz")
    print(f"Maximum Frequency: {cpu.max} MHz")